# BigQuery Proxy API

BigQuery 위에 얇게 얹는 단일 파일 Python HTTP 프록시. 큰 결과셋도 안전하게 흘려보낼 수 있도록 **커서 페이지네이션**과 **NDJSON 스트리밍**을 둘 다 제공한다.

- **단일 파일**: 핵심 로직은 `app.py` 한 파일.
- **FastAPI + Uvicorn**: async 핸들러 + `asyncio.to_thread`로 BQ 동기 클라이언트 위임.
- **두 가지 응답 모드**: `cursor`(JSON 페이지 + opaque 토큰) / `stream`(NDJSON).
- **인증**: ADC 기본, SA JSON 키 경로도 지원.
- **비용 가드**: `dry-run` 엔드포인트 + `maximum_bytes_billed` 옵션.
- **HMAC-서명 cursor**: 변조 차단 (개발 모드는 평문 허용).

---

## 1. 요구사항

- Python **3.10+** (`int | None`, `list[...]` 등 신문법 사용)
- GCP 프로젝트 + BigQuery API 사용 권한
- 인증 자격증명 (gcloud ADC 또는 서비스 계정 JSON)

---

## 2. 설치

```bash
# 가상환경
python3 -m venv .venv
source .venv/bin/activate

# 의존성
pip install --upgrade pip
pip install -r requirements.txt
```

> **IDE import 경고가 남는다면** VSCode/Cursor에서 인터프리터를 `.venv/bin/python`으로 바꿔주세요. (`Cmd+Shift+P` → "Python: Select Interpreter")

### 의존성 (requirements.txt)
```
fastapi>=0.110
uvicorn[standard]>=0.29
google-cloud-bigquery>=3.20
pydantic>=2.6
orjson>=3.10
```

---

## 3. GCP 인증

### 방법 A — 본인 계정 (개발용)
```bash
gcloud auth application-default login
gcloud config set project <YOUR_GCP_PROJECT>
```

### 방법 B — 서비스 계정 키
1. GCP IAM에서 SA 생성 → 권한 부여:
   - `roles/bigquery.jobUser` (쿼리 실행)
   - `roles/bigquery.dataViewer` (대상 데이터셋 또는 테이블 단위)
2. JSON 키 다운로드.
3. 환경변수 또는 CLI 인자로 경로 지정:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/sa.json"
# 또는
python app.py --credentials /absolute/path/to/sa.json
```

---

## 4. 실행

```bash
# 가장 간단
python app.py --project my-gcp-proj --port 8080

# uvicorn 직접 (멀티 워커 등 세부 제어)
export BQ_PROJECT=my-gcp-proj
uvicorn app:app --host 0.0.0.0 --port 8080 --workers 1
```

확인:
```bash
curl -s http://localhost:8080/healthz | jq .
# {"ok": true, "bq_project": "my-gcp-proj"}
```

---

## 5. 환경변수

| 변수 | 필수 | 기본값 | 설명 |
|---|---|---|---|
| `BQ_PROJECT` | △ | (ADC 기본) | 사용할 GCP 프로젝트. `--project`로도 지정 가능 |
| `GOOGLE_APPLICATION_CREDENTIALS` | × | — | SA JSON 경로 (표준 ADC) |
| `BQ_CREDENTIALS_FILE` | × | — | 같은 용도. 우선순위: 이쪽이 위. `--credentials`로도 지정 |
| `CURSOR_SECRET` | ◎ 운영 | — | 커서 HMAC-SHA256 서명 키. 미설정 시 평문 base64 + 시작 경고 |
| `BQ_DEFAULT_PAGE_SIZE` | × | `1000` | 요청에 `page_size` 없을 때 기본값 |
| `BQ_MAX_PAGE_SIZE` | × | `100000` | 허용 가능한 page_size 상한 |
| `BQ_MAX_CONCURRENCY` | × | `16` | 동시 BQ 호출 캡 (asyncio.Semaphore) |
| `BQ_MAX_BYTES_BILLED` | × | — | 쿼리당 최대 처리 바이트 (비용 가드). 요청 명시값이 없을 때 적용 |
| `BQ_REQUEST_TIMEOUT_S` | × | `300` | BQ `result()` 타임아웃 (초) |
| `HOST` | × | `0.0.0.0` | 바인드 호스트 |
| `PORT` | × | `8080` | 포트 |
| `LOG_LEVEL` | × | `info` | `debug`/`info`/`warning`/`error` |

`◎` 운영 권장, `△` 미지정 시 ADC가 추론, `×` 선택.

### `.env` 예시
```bash
BQ_PROJECT=my-gcp-proj
GOOGLE_APPLICATION_CREDENTIALS={PATH}/sa.json
CURSOR_SECRET=please-change-me-to-a-long-random-string
BQ_DEFAULT_PAGE_SIZE=1000
BQ_MAX_PAGE_SIZE=50000
BQ_MAX_CONCURRENCY=16
BQ_MAX_BYTES_BILLED=10737418240   # 10 GiB
BQ_REQUEST_TIMEOUT_S=300
LOG_LEVEL=info
```
로드 (한 줄):
```bash
set -a; source .env; set +a
```

---

## 6. API 사용법

모든 엔드포인트는 표준 JSON. 응답 헤더에 `X-Request-ID`가 항상 포함되며, 클라이언트가 `X-Request-ID`를 보내면 그대로 전파된다.

### 6.1 `POST /v1/query` — 쿼리 실행

요청 바디:
```jsonc
{
  "sql": "SELECT name, number FROM `bigquery-public-data.usa_names.usa_1910_2013` WHERE state = @st LIMIT 5000",
  "params": [
    { "name": "st", "type": "STRING", "value": "TX" }
  ],
  "location": "US",            // 선택 (멀티리전 사용 시 명시 권장)
  "page_size": 1000,           // 선택 (기본 BQ_DEFAULT_PAGE_SIZE)
  "max_bytes_billed": 1073741824, // 선택 (1 GiB)
  "use_query_cache": true,
  "mode": "cursor"             // "cursor" | "stream"
}
```

#### `mode: "cursor"` 응답
```json
{
  "schema": [
    { "name": "name", "type": "STRING", "mode": "NULLABLE", "fields": null },
    { "name": "number", "type": "INTEGER", "mode": "NULLABLE", "fields": null }
  ],
  "rows": [ { "name": "James", "number": 12345 }, ... ],
  "next_cursor": "eyJ2IjoxLCJqIjoi...",   // 더 이상 없으면 null
  "total_rows": 5000,
  "job_id": "bquxjob_..."
}
```

#### `mode: "stream"` 응답 — `application/x-ndjson`
- **첫 줄**: 메타
  ```json
  {"_meta":{"schema":[...],"job_id":"...","project":"...","location":"US","total_rows":5000}}
  ```
- **이후 각 줄**: 한 행
  ```json
  {"name":"James","number":12345}
  ```
- **에러 발생 시 마지막 줄**:
  ```json
  {"_error":{"code":"upstream_error","message":"..."}}
  ```

> 스트리밍은 `page_size` 만큼만 메모리에 잡고 나머지는 흘려보낸다. 매우 큰 결과에 적합.

#### 쿼리 파라미터 타입
- 스칼라: `STRING`, `INT64`, `FLOAT64`, `BOOL`, `NUMERIC`, `BIGNUMERIC`, `TIMESTAMP`, `DATE`, `DATETIME`, `TIME`, `BYTES` 등 BQ 표준 타입.
- 배열: `value`를 list로 주면 `ARRAY<type>`으로 전달됨.
- ⚠ SQL 문자열 보간 절대 금지. 파라미터 바인딩만 사용.

### 6.2 `GET /v1/query/next` — 다음 페이지 (cursor 모드)

```bash
curl -s "http://localhost:8080/v1/query/next?cursor=<TOKEN>&page_size=1000" | jq .
```
- `cursor` (필수) — 직전 응답의 `next_cursor` 그대로.
- `page_size` (선택) — 생략 시 cursor에 인코딩된 값 사용.

응답 스키마는 `POST /v1/query` cursor 응답과 동일.

만료 케이스 (BQ 잡 결과가 사라짐, 보통 24시간 후) → `410 Gone`:
```json
{ "detail": { "code": "cursor_expired", "message": "..." } }
```

### 6.3 `POST /v1/query/dry-run` — 비용 사전 체크

요청 바디는 `POST /v1/query`와 동일 (단, `mode`/`page_size` 등은 무시됨).

응답:
```json
{
  "schema": [ ... ],
  "total_bytes_processed": 12345678,
  "referenced_tables": ["proj.dataset.table"],
  "statement_type": "SELECT"
}
```

### 6.4 `GET /healthz`
```json
{ "ok": true, "bq_project": "my-gcp-proj" }
```

---

## 7. 호출 예시

### 작은 쿼리 (cursor)
```bash
curl -s -X POST http://localhost:8080/v1/query \
  -H 'content-type: application/json' \
  -d '{
        "sql": "SELECT name, number FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 5000",
        "page_size": 1000,
        "mode": "cursor"
      }' | jq .
```

다음 페이지:
```bash
TOKEN="eyJ2..."  # 직전 응답의 next_cursor
curl -s "http://localhost:8080/v1/query/next?cursor=$TOKEN" | jq .
```

쉘로 끝까지 페이지 순회:
```bash
TOKEN=$(curl -s -X POST http://localhost:8080/v1/query \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 50000","page_size":5000}' \
  | tee /tmp/p1.json | jq -r '.next_cursor')

while [ "$TOKEN" != "null" ] && [ -n "$TOKEN" ]; do
  TOKEN=$(curl -s "http://localhost:8080/v1/query/next?cursor=$TOKEN" \
            | tee -a /tmp/pages.json | jq -r '.next_cursor')
done
```

### 대용량 (NDJSON 스트리밍)
```bash
curl -N -X POST http://localhost:8080/v1/query \
  -H 'content-type: application/json' \
  -d '{
        "sql": "SELECT * FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 200000",
        "page_size": 5000,
        "mode": "stream"
      }' \
  | head -n 5
```

Python 클라이언트:
```python
import httpx, json
with httpx.stream("POST", "http://localhost:8080/v1/query",
                  json={"sql": "...", "mode": "stream", "page_size": 5000},
                  timeout=None) as r:
    for line in r.iter_lines():
        if not line:
            continue
        rec = json.loads(line)
        if "_meta" in rec:
            schema = rec["_meta"]["schema"]
            continue
        if "_error" in rec:
            raise RuntimeError(rec["_error"])
        process(rec)
```

### Dry-run (비용 확인)
```bash
curl -s -X POST http://localhost:8080/v1/query/dry-run \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT * FROM `bigquery-public-data.usa_names.usa_1910_2013`"}' \
  | jq .
```

### 파라미터 쿼리
```bash
curl -s -X POST http://localhost:8080/v1/query \
  -H 'content-type: application/json' \
  -d '{
        "sql": "SELECT name, SUM(number) AS total FROM `bigquery-public-data.usa_names.usa_1910_2013` WHERE state IN UNNEST(@states) GROUP BY name ORDER BY total DESC LIMIT 100",
        "params": [
          { "name": "states", "type": "STRING", "value": ["TX","NY","CA"] }
        ]
      }' | jq .
```

---

## 8. 에러 매핑

| HTTP | code | 의미 |
|---|---|---|
| 400 | `bad_request` | 잘못된 SQL 등 BQ 400 |
| 400 | `invalid_cursor` | 커서 변조/형식 오류 |
| 403 | `forbidden` | 권한 부족 |
| 404 | `not_found` | 테이블/데이터셋 없음 |
| 410 | `cursor_expired` | BQ 잡 결과가 더 이상 없음 (~24h 후) |
| 429 | `rate_limited` | BQ 쿼터/속도 제한 |
| 502 | `upstream_error` | 분류되지 않은 BQ API 에러 |
| 503 | `unavailable` | BQ 일시 불가 |
| 500 | `internal_error` | 서버 내부 예외 |

---

## 9. 동시성 / 성능

- 모든 핸들러는 `async def`. 동기 BQ 호출은 `asyncio.to_thread`로 스레드풀에 위임 → **단일 프로세스에서도 IO 동시성** 확보.
- BQ `Client`는 lifespan에서 1회 생성·재사용 (멀티스레드 안전).
- `BQ_MAX_CONCURRENCY` 세마포어로 동시 BQ 호출 수 캡 (기본 16).
- 더 큰 처리량이 필요하면 `uvicorn --workers N`으로 수평 확장. 코드는 그대로.

벤치 예시:
```bash
hey -c 32 -n 200 http://localhost:8080/healthz
```

---

## 10. 운영 체크리스트

- [ ] `CURSOR_SECRET`을 32+자 랜덤으로 설정 (`openssl rand -hex 32`)
- [ ] `BQ_MAX_BYTES_BILLED`로 쿼리당 비용 상한
- [ ] `BQ_MAX_PAGE_SIZE`를 너무 크게 두지 않기 (응답 메모리)
- [ ] SA에 최소 권한만 (`bigquery.jobUser` + 필요한 데이터셋 `dataViewer`)
- [ ] 인입 API 자체에 인증 (Bearer 토큰 등) — **이번 버전엔 미포함**, 필요 시 미들웨어로 추가
- [ ] 리버스 프록시(nginx/Cloud Run/ALB) 뒤에서 TLS 종단
- [ ] `LOG_LEVEL=info` 이상으로 운영하고 `X-Request-ID`로 추적

---

## 11. 보안 메모

- SQL 인젝션: 모든 동적 값은 **파라미터 바인딩**으로 전달. `req.sql`에 직접 보간 금지.
- 커서: HMAC-SHA256 서명 (`CURSOR_SECRET` 설정 시). 검증 실패 시 400 `invalid_cursor`.
- 인증/인가: 이번 버전은 BQ 측 자격증명만 사용. 인입 API 측 인증은 별도 미들웨어로 추가 필요.

---

## 12. 비포함 (향후 확장)

- BigQuery **Storage Read API** 기반 고속 다운로드 모드
- 인입 API용 Bearer 토큰 / OAuth 인증 미들웨어
- SQL 화이트리스트 / 파서 검증
- Dockerfile, Cloud Run 매니페스트
- 결과 CSV / Arrow 응답 포맷

---

## 13. 디렉터리 구조

```
agent_os_bigquery_connector/
├── app.py               # 단일 파일 서비스
├── requirements.txt
└── README.md            # 본 문서
```
