# Architecture

## 패키지 구성

| 경로 | 역할 |
| --- | --- |
| `cmd/ntick` | 서버. 수집, REST, WebSocket, 시작 시 `CatchUp`, 장 마감 스케줄러 |
| `cmd/ntickctl` | 운영 도구: `closeout`(장 마감 확정·점검), `rejudge`(과거 일자 일괄 재판정) |
| `cmd/mockfeed` | 목업 시세 발생기 (TCP 서버, 정답 파일 출력) |
| `cmd/tclient` | 테스트 클라이언트 CLI + 자동 검증 |
| `internal/wire` | 바이너리 고정 길이 전문 encode/decode, symbol 검증. 실제 전문과 다를 수 있어 어댑터로 격리 |
| `internal/tick` | `Tick` 타입, `Rule`/`Filter` (수신 시 invalid 판정) |
| `internal/store` | 일자·종목 DB 스키마(`schema.sql`), 원자적 파일 생성, pragma(DSN), `OpenRO`, `candle_1m` 누적 |
| `internal/ingest` | 피드 어댑터, 종목별 writer worker, 6.1 트랜잭션, 1m 집계, 커밋 이벤트 발행 |
| `internal/query` | n-tick 조회, 시간 캔들 롤업, 스냅샷 읽기 |
| `internal/api` | REST 핸들러 |
| `internal/stream` | WebSocket 실시간 갱신, 브로커, 진행 중 캔들 상태기계 |
| `internal/dayindex` | `meta.db` `day_index`, 장 마감 점검, `CatchUp`, 스케줄러 |
| `internal/rejudge` | 오늘 이전 일자의 invalid 일괄 재판정 |
| `internal/oracle` | 정답 파일에서 독립 계산하는 기대값 (시험 전용) |

의존 방향: `cmd/* -> api, stream, ingest, query, dayindex, rejudge -> store, tick, wire`. `tick`은 어떤 내부 패키지도 import하지 않는다.

## 데이터 흐름

```
mockfeed --TCP--> feed adapter (wire.Decode -> symbol·ts 검증 -> tick.Tick)
  -> Filter.Valid(t)          // valid 플래그 결정, 도착 순서대로
  -> hash(symbol) -> worker 채널 (worker 1개가 여러 종목 담당, 종목당 writer는 항상 1개)
  -> micro-batch -> BEGIN IMMEDIATE; INSERT ticks; UPSERT day_stats; UPSERT candle_1m; COMMIT
  -> 커밋 후 event{symbol, date, 신규 유효 tick, T, last_raw_seq} -> stream 브로커 -> WS 구독자
```

- 필터는 worker에서 호출한다. 상태 있는 규칙이 종목별 도착 순서를 보므로 락이 필요 없다. worker마다 별도 `Filter`를 둔다.
- 일자 파일은 `ts`의 현지 날짜로 결정한다. 파일당 `*sql.DB`는 `MaxOpenConns(1)`이다.

## SQLite 드라이버: `modernc.org/sqlite` (pure Go)

내장 SQLite 3.53.4. cgo가 필요 없어 교차 컴파일과 단일 바이너리 배포가 쉽다. mattn/go-sqlite3(cgo)와 기능 결과는 동일했고 처리량 비교는 `docs/perf-report.md`를 본다. pragma(`journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000`)는 `store.Pragmas`가 단일 출처이며 DSN(`_pragma`)으로 모든 연결에 적용된다.

## Ingest

- `internal/ingest`: `ingest.go`(해시 라우팅, TCP 읽기, 백오프 재접속, 입력 검증), `worker.go`(micro-batch, 파일별 분할, 6.1 트랜잭션, worker별 LRU 연결 풀). worker 수는 CPU 수, 배치는 500 tick 또는 50 ms.
- 입력 검증: symbol은 `^[A-Za-z0-9_-]{1,12}$`, `ts`는 [2000-01-01, 2100-01-01) UTC. 어긋난 전문은 디렉터리·파일을 만들기 전에 버리고 초당 1줄로 로그한다.
- 파일 생성은 원자적이다. `{symbol}.db.tmp`에 스키마를 적용한 뒤 `rename`하므로 읽기 쪽은 스키마 없는 파일을 보지 않는다.
- FD: 연결 풀 크기는 `RLIMIT_NOFILE`의 절반을 worker × 3 파일로 나눠 시작 시 정하고(4~64) 로그에 남긴다.
- 커밋 실패는 한 번 재시도하고 그래도 실패하면 프로세스를 종료한다 (`log.Fatal`).
- 장 마감: 시작 시 `dayindex.CatchUp`(오늘 이전 일자 확정), `-close HH:MM`에 `Closeout`. 드리프트는 로그와 `ntickctl` 종료 코드로만 알리고 자동 복구는 하지 않는다. 조회의 T_d는 항상 일자 파일의 `day_stats`를 읽는다 (`day_index`는 확정 기록·점검용).
- 재판정: 오늘 이전 일자 파일에서 현재 수신 필터를 raw_seq 순서로 다시 적용하고, 한 트랜잭션에서 `valid`, `valid_count`, `candle_1m`을 갱신한다.

## API

`internal/api.Handler(dataDir, loc)`가 `GET /candles`를 서비스한다 (stdlib `net/http`). `cmd/ntick -http :8080`이 수집 프로세스 안에서 띄우고 수집과 함께 graceful shutdown 한다. `ReadHeaderTimeout` 5초, `IdleTimeout` 60초.

- `GET /candles?symbol=S&type=tick&n=N&m=M`: 최신순 n-tick 캔들 (`n` 1..10000, `m` 1..1000).
- `GET /candles?symbol=S&type=time&interval=5m&from=MS&to=MS`: 시간 오름차순, 시작이 `[from, to)`. `interval`은 `<k>m|h|d`, 최대 1d. `from < to`, epoch ms. 자정 정렬 버킷은 자정을 넘지 않고 틱 없는 구간은 생략한다. 미완성(`partial`)은 구간 끝이 종목의 보유 데이터 최신 ts보다 뒤인 경우다.
- 응답: `{"symbol", "type", "candles": [...]}`. 비어 있으면 `[]`. 필드는 snake_case 정수(최소 단위, 시각 epoch ms).
  - time: `start, open, high, low, close, volume, tick_count, partial`
  - tick: `date (YYYYMMDD), open, high, low, close, volume, tick_count, partial`
- 오류: 본문 `{"error": "..."}`. 400은 누락·잘못된·상한 초과 파라미터, 알 수 없는 `type`, 패턴에 맞지 않는 `symbol`(파일 경로에 쓰이므로), `from`이 최신 30개 파일 범위 밖(`query.ErrRangeTooLarge`). 500은 읽기 실패(상세는 서버 로그에만). 비 GET은 405(JSON).
- 데이터 디렉터리·종목이 없으면 항상 200과 빈 `candles`다.
- 조회는 `r.Context()`를 따라가 클라이언트가 끊으면 취소된다. 상한은 일자 파일 30개이며 캐시는 없다.

## Realtime

`WS /candles/stream?symbol=S&type=tick&n=N` 또는 `...&type=time&interval=5m` (검증은 REST와 같고 업그레이드 전에 400 JSON). `internal/stream`이 REST와 같은 mux에서 서비스한다.

- 훅: `Ingester.OnCommit(fn)`은 유효 tick이 추가된 파일 트랜잭션이 커밋될 때마다 `ingest.Event{Symbol, Date, Ticks(유효만, raw_seq 순, Seq 포함), T, LastSeq}`로 호출된다. `stream.Broker.Publish`가 비차단으로 구독자 버퍼(256 이벤트)에 넣는다. 뒤처진 구독자는 close code 1013과 사유로 끊고(재접속하면 새 snapshot) writer는 기다리지 않는다. 클라이언트 쓰기 타임아웃은 10초.
- 연결: 브로커에 먼저 구독한 뒤, 유효 tick이 있는 최신 일자 파일을 한 읽기 트랜잭션(`last_raw_seq` 상한)으로 읽어 snapshot을 만든다. 같은 일자의 `Seq <= last_raw_seq` 이벤트는 건너뛰므로 누락도 중복도 없다. 이후에는 이벤트만으로 갱신하고 DB를 읽지 않는다.
- 메시지: `snapshot`(캔들 또는 null), `update`, `complete`, 시간 스트림에서만 `invalidate {start}`(진행 중 버킷보다 이른 버킷을 늦은 tick이 건드린 경우). tick `complete`는 `T`가 n의 배수가 되는 즉시 보내고(그 시점의 snapshot은 null), 더 새로운 일자의 tick은 이전 일자의 부분 캔들을 완성시킨다. tick 캔들에는 `date`와 일자 내 0부터의 순번 `index`가 있다. 진행 중인 날 이전 일자의 늦은 tick은 tick 스트림에서 무시한다.
- 종료: `cmd/ntick`은 HTTP 수락 중단, ingest 종료(flush), `Broker.Close()`(close frame 전송, 핸들러 대기) 순으로 멈춘다.
- 한계: 프로세스당 구독자 수 상한과 인증이 없다. 브라우저 클라이언트는 라이브러리 기본 same-origin 검사를 따른다.
