"""
BigQuery Proxy API — single-file FastAPI service.

Endpoints
---------
  POST /v1/query           Submit SQL. mode="cursor" returns one page + opaque
                           next_cursor; mode="stream" streams NDJSON.
  GET  /v1/query/next      Fetch the next page using a cursor returned earlier.
  POST /v1/query/dry-run   Estimate bytes processed without running the query.
  GET  /healthz            Liveness check (also reports the BQ project in use).

Run
---
  python app.py --project my-proj --port 8080
or
  uvicorn app:app --host 0.0.0.0 --port 8080

Env knobs
---------
  BQ_PROJECT, BQ_CREDENTIALS_FILE, GOOGLE_APPLICATION_CREDENTIALS,
  CURSOR_SECRET, BQ_DEFAULT_PAGE_SIZE, BQ_MAX_PAGE_SIZE, BQ_MAX_CONCURRENCY,
  BQ_MAX_BYTES_BILLED, BQ_REQUEST_TIMEOUT_S
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import datetime as dt
import decimal
import hashlib
import hmac
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Iterable, Iterator, Literal, Optional

import orjson
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from google.api_core import exceptions as gax
from google.cloud import bigquery
from google.oauth2 import service_account
from pydantic import BaseModel, field_validator

logger = logging.getLogger("bq_proxy")


# --------------------------- Config ---------------------------
@dataclass
class Settings:
    project: Optional[str]
    credentials_file: Optional[str]
    cursor_secret: Optional[str]
    default_page_size: int
    max_page_size: int
    max_concurrency: int
    default_max_bytes_billed: Optional[int]
    request_timeout_s: float

    @classmethod
    def from_env(cls) -> "Settings":
        mbb_env = os.getenv("BQ_MAX_BYTES_BILLED")
        return cls(
            project=os.getenv("BQ_PROJECT"),
            credentials_file=(
                os.getenv("BQ_CREDENTIALS_FILE")
                or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            ),
            cursor_secret=os.getenv("CURSOR_SECRET"),
            default_page_size=int(os.getenv("BQ_DEFAULT_PAGE_SIZE", "1000")),
            max_page_size=int(os.getenv("BQ_MAX_PAGE_SIZE", "100000")),
            max_concurrency=int(os.getenv("BQ_MAX_CONCURRENCY", "16")),
            default_max_bytes_billed=int(mbb_env) if mbb_env else None,
            request_timeout_s=float(os.getenv("BQ_REQUEST_TIMEOUT_S", "300")),
        )


# --------------------------- Auth & client ---------------------------
def make_bq_client(settings: Settings) -> bigquery.Client:
    creds = None
    cf = settings.credentials_file
    if cf and os.path.exists(cf):
        creds = service_account.Credentials.from_service_account_file(cf)
    return bigquery.Client(project=settings.project, credentials=creds)


# --------------------------- Cursor codec ---------------------------
# Opaque cursor payload (JSON):
#   { "v": 1, "j": job_id, "p": project, "l": location,
#     "t": page_token, "n": page_size }
# Optionally HMAC-signed when CURSOR_SECRET is set: "<body>.<sig>".

def _b64url_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def encode_cursor(payload: dict, secret: Optional[str]) -> str:
    raw = orjson.dumps(payload)
    body = _b64url_encode(raw)
    if not secret:
        return body
    sig = hmac.new(secret.encode(), raw, hashlib.sha256).digest()
    return f"{body}.{_b64url_encode(sig)}"


def decode_cursor(token: str, secret: Optional[str]) -> dict:
    try:
        if "." in token:
            body, sig = token.split(".", 1)
            raw = _b64url_decode(body)
            if not secret:
                raise ValueError("server has no cursor secret configured")
            expected = hmac.new(secret.encode(), raw, hashlib.sha256).digest()
            if not hmac.compare_digest(expected, _b64url_decode(sig)):
                raise ValueError("bad signature")
        else:
            if secret:
                raise ValueError("missing cursor signature")
            raw = _b64url_decode(token)
        payload = orjson.loads(raw)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_cursor", "message": str(e)},
        )
    if payload.get("v") != 1 or "j" not in payload or "t" not in payload:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_cursor", "message": "malformed cursor payload"},
        )
    return payload


# --------------------------- Models ---------------------------
class QueryParam(BaseModel):
    name: str
    type: str  # STRING, INT64, FLOAT64, BOOL, NUMERIC, BIGNUMERIC, TIMESTAMP, DATE, ...
    value: Any = None  # scalar, or list for ARRAY<type>


class QueryRequest(BaseModel):
    sql: str
    params: Optional[list[QueryParam]] = None
    location: Optional[str] = None
    page_size: Optional[int] = None
    max_bytes_billed: Optional[int] = None
    use_query_cache: bool = True
    mode: Literal["cursor", "stream"] = "cursor"

    @field_validator("sql")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("sql must not be empty")
        return v


# --------------------------- Helpers ---------------------------
def _serialize_field(field: bigquery.SchemaField) -> dict:
    return {
        "name": field.name,
        "type": field.field_type,
        "mode": field.mode,
        "fields": [_serialize_field(f) for f in field.fields] if field.fields else None,
    }


def _serialize_schema(schema: Iterable[bigquery.SchemaField]) -> list[dict]:
    return [_serialize_field(f) for f in (schema or [])]


def _value_default(o: Any) -> Any:
    if isinstance(o, (dt.datetime, dt.date, dt.time)):
        return o.isoformat()
    if isinstance(o, dt.timedelta):
        return o.total_seconds()
    if isinstance(o, decimal.Decimal):
        return str(o)
    if isinstance(o, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(o)).decode("ascii")
    if isinstance(o, uuid.UUID):
        return str(o)
    raise TypeError(f"object of type {type(o).__name__} is not JSON-serializable")


def _row_to_jsonable(row: bigquery.Row) -> dict:
    # orjson roundtrip applies _value_default to nested types.
    return orjson.loads(orjson.dumps(dict(row.items()), default=_value_default))


def _build_query_job_config(
    req: QueryRequest, settings: Settings, *, dry_run: bool = False
) -> bigquery.QueryJobConfig:
    params: list = []
    if req.params:
        for p in req.params:
            if isinstance(p.value, list):
                params.append(bigquery.ArrayQueryParameter(p.name, p.type, p.value))
            else:
                params.append(bigquery.ScalarQueryParameter(p.name, p.type, p.value))
    job_config = bigquery.QueryJobConfig(
        query_parameters=params,
        use_query_cache=req.use_query_cache,
        dry_run=dry_run,
    )
    mbb = (
        req.max_bytes_billed
        if req.max_bytes_billed is not None
        else settings.default_max_bytes_billed
    )
    if mbb is not None:
        job_config.maximum_bytes_billed = mbb
    return job_config


def _map_google_error(e: gax.GoogleAPICallError) -> HTTPException:
    msg = str(e)
    if isinstance(e, gax.NotFound):
        return HTTPException(404, {"code": "not_found", "message": msg})
    if isinstance(e, gax.Forbidden):
        return HTTPException(403, {"code": "forbidden", "message": msg})
    if isinstance(e, gax.BadRequest):
        return HTTPException(400, {"code": "bad_request", "message": msg})
    if isinstance(e, (gax.TooManyRequests, gax.ResourceExhausted)):
        return HTTPException(
            429, {"code": "rate_limited", "message": msg}, headers={"Retry-After": "1"}
        )
    if isinstance(e, gax.ServiceUnavailable):
        return HTTPException(
            503, {"code": "unavailable", "message": msg}, headers={"Retry-After": "1"}
        )
    return HTTPException(502, {"code": "upstream_error", "message": msg})


class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, default=_value_default)


# --------------------------- BigQuery adapter (sync) ---------------------------
def _run_query_first_page(
    client: bigquery.Client, req: QueryRequest, settings: Settings
) -> dict:
    page_size = req.page_size or settings.default_page_size
    page_size = max(1, min(page_size, settings.max_page_size))
    job_config = _build_query_job_config(req, settings)
    job = client.query(req.sql, job_config=job_config, location=req.location)
    iterator = job.result(page_size=page_size, timeout=settings.request_timeout_s)
    page = next(iterator.pages, None)
    rows = [_row_to_jsonable(r) for r in page] if page is not None else []
    next_token = getattr(iterator, "next_page_token", None) or None
    return {
        "schema": _serialize_schema(iterator.schema or job.schema or []),
        "rows": rows,
        "next_page_token": next_token,
        "total_rows": getattr(iterator, "total_rows", None),
        "job_id": job.job_id,
        "project": job.project,
        "location": job.location,
        "page_size": page_size,
    }


def _fetch_next_page(
    client: bigquery.Client,
    *,
    job_id: str,
    project: Optional[str],
    location: Optional[str],
    page_token: str,
    page_size: int,
    settings: Settings,
) -> dict:
    page_size = max(1, min(page_size, settings.max_page_size))
    try:
        job = client.get_job(job_id, project=project, location=location)
        iterator = job.result(
            page_size=page_size,
            page_token=page_token,
            timeout=settings.request_timeout_s,
        )
    except gax.NotFound:
        raise HTTPException(
            410,
            {
                "code": "cursor_expired",
                "message": "BigQuery job/results no longer available",
            },
        )
    page = next(iterator.pages, None)
    rows = [_row_to_jsonable(r) for r in page] if page is not None else []
    next_token = getattr(iterator, "next_page_token", None) or None
    return {
        "schema": _serialize_schema(iterator.schema or job.schema or []),
        "rows": rows,
        "next_page_token": next_token,
        "total_rows": getattr(iterator, "total_rows", None),
        "job_id": job.job_id,
        "project": job.project,
        "location": job.location,
        "page_size": page_size,
    }


def _dry_run(client: bigquery.Client, req: QueryRequest, settings: Settings) -> dict:
    job_config = _build_query_job_config(req, settings, dry_run=True)
    job_config.use_query_cache = False
    job = client.query(req.sql, job_config=job_config, location=req.location)
    referenced = [
        f"{t.project}.{t.dataset_id}.{t.table_id}" for t in (job.referenced_tables or [])
    ]
    return {
        "schema": _serialize_schema(job.schema or []),
        "total_bytes_processed": int(job.total_bytes_processed or 0),
        "referenced_tables": referenced,
        "statement_type": job.statement_type,
    }


def _stream_pages(
    client: bigquery.Client, req: QueryRequest, settings: Settings
) -> Iterator[tuple[str, Any]]:
    """Yield ('meta', meta) once, then ('row', dict) per row, until exhausted."""
    page_size = req.page_size or settings.default_page_size
    page_size = max(1, min(page_size, settings.max_page_size))
    job_config = _build_query_job_config(req, settings)
    job = client.query(req.sql, job_config=job_config, location=req.location)
    iterator = job.result(page_size=page_size, timeout=settings.request_timeout_s)
    yield "meta", {
        "schema": _serialize_schema(iterator.schema or job.schema or []),
        "job_id": job.job_id,
        "project": job.project,
        "location": job.location,
        "total_rows": getattr(iterator, "total_rows", None),
    }
    for page in iterator.pages:
        for row in page:
            yield "row", _row_to_jsonable(row)


# --------------------------- NDJSON streaming bridge ---------------------------
async def _ndjson_stream(
    bq: bigquery.Client,
    req: QueryRequest,
    settings: Settings,
    sem: asyncio.Semaphore,
) -> AsyncIterator[bytes]:
    async with sem:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=4)
        sentinel = object()
        stop_event = threading.Event()

        def _put(item: Any) -> None:
            fut = asyncio.run_coroutine_threadsafe(queue.put(item), loop)
            try:
                fut.result()
            except Exception:
                # Loop may be tearing down; nothing to do.
                pass

        def producer() -> None:
            try:
                for item in _stream_pages(bq, req, settings):
                    if stop_event.is_set():
                        return
                    _put(item)
            except gax.GoogleAPICallError as e:
                if not stop_event.is_set():
                    _put(("error", {"code": "upstream_error", "message": str(e)}))
            except Exception as e:  # noqa: BLE001
                if not stop_event.is_set():
                    _put(("error", {"code": "internal_error", "message": repr(e)}))
            finally:
                _put(sentinel)

        producer_fut = loop.run_in_executor(None, producer)
        try:
            while True:
                item = await queue.get()
                if item is sentinel:
                    break
                kind, payload = item
                if kind == "meta":
                    yield orjson.dumps({"_meta": payload}, default=_value_default) + b"\n"
                elif kind == "row":
                    yield orjson.dumps(payload, default=_value_default) + b"\n"
                elif kind == "error":
                    yield orjson.dumps({"_error": payload}, default=_value_default) + b"\n"
                    break
        finally:
            stop_event.set()
            # Drain so a blocked producer can wake up and exit.
            try:
                while True:
                    queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                await asyncio.wait_for(producer_fut, timeout=2.0)
            except asyncio.TimeoutError:
                pass


# --------------------------- Lifespan ---------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Settings = app.state.settings
    if not settings.cursor_secret:
        logger.warning(
            "CURSOR_SECRET is not set; cursors will be unsigned (development mode)"
        )
    client = make_bq_client(settings)
    app.state.bq = client
    app.state.semaphore = asyncio.Semaphore(settings.max_concurrency)
    logger.info(
        "bq client ready (project=%s, max_concurrency=%d, default_page_size=%d)",
        client.project,
        settings.max_concurrency,
        settings.default_page_size,
    )
    try:
        yield
    finally:
        client.close()


# --------------------------- App factory ---------------------------
def _cursor_from_result(result: dict, secret: Optional[str]) -> Optional[str]:
    if not result["next_page_token"]:
        return None
    return encode_cursor(
        {
            "v": 1,
            "j": result["job_id"],
            "p": result["project"],
            "l": result["location"],
            "t": result["next_page_token"],
            "n": result["page_size"],
        },
        secret,
    )


def build_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(
        title="BigQuery Proxy",
        version="0.1.0",
        lifespan=lifespan,
        default_response_class=ORJSONResponse,
    )
    app.state.settings = settings

    @app.middleware("http")
    async def _request_id(request: Request, call_next):
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = rid
        start = time.perf_counter()
        try:
            resp = await call_next(request)
        except Exception:
            logger.exception("unhandled error rid=%s", rid)
            return ORJSONResponse(
                status_code=500,
                content={"detail": {"code": "internal_error", "message": "internal error"}},
                headers={"x-request-id": rid},
            )
        resp.headers["x-request-id"] = rid
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "rid=%s %s %s -> %s in %.1fms",
            rid,
            request.method,
            request.url.path,
            resp.status_code,
            elapsed_ms,
        )
        return resp

    @app.get("/healthz")
    async def healthz():
        bq: bigquery.Client = app.state.bq
        return {"ok": True, "bq_project": bq.project}

    @app.post("/v1/query")
    async def query(req: QueryRequest):
        bq: bigquery.Client = app.state.bq
        sem: asyncio.Semaphore = app.state.semaphore

        if req.mode == "stream":
            return StreamingResponse(
                _ndjson_stream(bq, req, settings, sem),
                media_type="application/x-ndjson",
            )

        async with sem:
            try:
                result = await asyncio.to_thread(
                    _run_query_first_page, bq, req, settings
                )
            except gax.GoogleAPICallError as e:
                raise _map_google_error(e)

        return ORJSONResponse(
            {
                "schema": result["schema"],
                "rows": result["rows"],
                "next_cursor": _cursor_from_result(result, settings.cursor_secret),
                "total_rows": result["total_rows"],
                "job_id": result["job_id"],
            }
        )

    @app.get("/v1/query/next")
    async def query_next(
        cursor: str = Query(...),
        page_size: Optional[int] = Query(default=None),
    ):
        bq: bigquery.Client = app.state.bq
        sem: asyncio.Semaphore = app.state.semaphore
        payload = decode_cursor(cursor, settings.cursor_secret)
        size = page_size or int(payload.get("n") or settings.default_page_size)

        async with sem:
            try:
                result = await asyncio.to_thread(
                    _fetch_next_page,
                    bq,
                    job_id=payload["j"],
                    project=payload.get("p"),
                    location=payload.get("l"),
                    page_token=payload["t"],
                    page_size=size,
                    settings=settings,
                )
            except gax.GoogleAPICallError as e:
                raise _map_google_error(e)

        return ORJSONResponse(
            {
                "schema": result["schema"],
                "rows": result["rows"],
                "next_cursor": _cursor_from_result(result, settings.cursor_secret),
                "total_rows": result["total_rows"],
                "job_id": result["job_id"],
            }
        )

    @app.post("/v1/query/dry-run")
    async def dry_run(req: QueryRequest):
        bq: bigquery.Client = app.state.bq
        sem: asyncio.Semaphore = app.state.semaphore
        async with sem:
            try:
                result = await asyncio.to_thread(_dry_run, bq, req, settings)
            except gax.GoogleAPICallError as e:
                raise _map_google_error(e)
        return ORJSONResponse(result)

    return app


# --------------------------- Entrypoint ---------------------------
app = build_app()


def _main() -> None:
    p = argparse.ArgumentParser(description="BigQuery proxy API")
    p.add_argument("--host", default=os.getenv("HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=int(os.getenv("PORT", "8080")))
    p.add_argument("--project", default=None, help="GCP project (BQ_PROJECT)")
    p.add_argument(
        "--credentials",
        default=None,
        help="Path to service-account JSON (BQ_CREDENTIALS_FILE)",
    )
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "info"))
    args = p.parse_args()

    if args.project:
        os.environ["BQ_PROJECT"] = args.project
    if args.credentials:
        os.environ["BQ_CREDENTIALS_FILE"] = args.credentials

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    import uvicorn

    uvicorn.run(
        "app:app",
        host=args.host,
        port=args.port,
        workers=args.workers,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    _main()
