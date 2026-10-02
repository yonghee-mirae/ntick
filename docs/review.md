# Review (Reviewer 감사 결과)

범위: `cmd/`, `internal/` 전체 코드 읽기, `go vet` 무경고, `go test -count=1 ./...` 전부 통과. `scripts/e2e.sh`는 실행하지 않았다(미검증). 줄 번호는 읽은 시점 기준이며 일부 파일은 다른 팀원이 수정 중이라 어긋날 수 있다.

## 1. PRD 추적표

"테스트"는 해당 요구가 깨지면 실패할 테스트만 적었다. 없으면 "없음".

| 요구 | 구현 | 테스트 | 상태 |
| --- | --- | --- | --- |
| FR-1 invalid 저장 + valid 플래그 | `ingest/worker.go:write` (valid 0/1 insert) | `ingest TestIngestOverTCP`, `rejudge TestRejudgeRestoresFreshIngest` | OK |
| FR-2 invalid 제외 (OHLCV, 개수, 경계) | `write` (candle/nValid는 valid만), `query.readDayTx` (`valid=1`) | `query TestAgainstOracle`, `TestTimeAgainstBruteForce` | OK |
| FR-3 1m 기본 + 롤업 | `store.UpsertCandle`, `query/time.go:rollup` | `TestRollup`, `TestTimeAgainstBruteForce` | OK |
| FR-4 `[시작,종료)`, 현지 경계 | `ingest.split` (date/minute), `query.Time` | `TestSplitByDate`, `TestTime`. 단 `query.Time`은 `Asia/Seoul` 하드코딩, `-tz` 무시 (D-5) | partial |
| FR-5 진행 중 캔들 미완성 표시 | `query.Time` (`Partial = 종료 > 종목 최신 ts`), tick `Partial = TickCount<n` | `TestTimeAgainstBruteForce`(oracle 비교). wall-clock 기준이 아니라 데이터 기준이라 장 마감 후 마지막 버킷이 영구 partial | partial |
| FR-5a 빈 구간 생략 | `readMinutes` (`tick_count>0`), `rollup`은 행 있는 버킷만 생성 | `TestRollup` | OK |
| FR-6 (n,m) 최근→과거 m개 | `query.NTick`, `api.candles` | `TestAgainstOracle`, `api TestTick` | OK |
| FR-7 일자 첫 유효 tick 기준 n개 고정 | `readDayTx` (r건 + n건씩) | `TestAgainstOracle`, `TestPlan` | OK |
| FR-8 일자별 마지막 캔들 미완, 합치지 않음 | `readDayTx`(일자 단위), `plan` | `TestAgainstOracle`, `TestConcurrentWriter` | OK |
| FR-9 전일 연속, 빈 날 건너뜀 | `NTick` 루프, `listDays` | `TestAgainstOracle`, `TestMaxDays`. 구현은 `day_index`가 아니라 파일 시스템 스캔 (D-9) | partial |
| FR-10 부족하면 있는 만큼 | `NTick` | `TestLimits`/`TestNoData200` | OK |
| FR-11 진행 중 캔들 푸시 | `ingest.OnCommit` -> `stream.Broker.Publish` -> `handler.ServeHTTP` | `TestIntegration`, `TestSnapshotDedup`, `TestTickMachineMatchesOracle`, `TestTimeMachineMatchesOracle` | OK |
| FR-12 T%n==0이면 다음 tick부터 새 캔들 | `stream/machine.go:tickMachine.apply`, `snapshot.go:load` | `TestTickMachineMatchesOracle`, `TestIntegration` | OK |
| 4.2 파일 분할, 한 파일 한 worker | `Put` (fnv 해시), `worker.open` | `TestIngestOverTCP`. 라우팅 불변(같은 종목은 같은 worker) 전용 테스트 없음 | partial |
| 4.2 raw_seq = rowid | `schema.sql`, `write` | `store TestSQLite`, `TestIngestOverTCP` | OK |
| 4.2 정렬 기준(n-tick raw_seq / time ts+raw_seq) | `Candle.Add`, upsert `<` / `>=` | `TestCandleOpenClose` | OK |
| 4.2 같은 파일(day_stats, candle_1m) | `schema.sql` | `TestSQLite` | OK |
| 4.2 day_index (마감 시 확정, 읽기 경로에 사용) | `dayindex.Closeout/CatchUp/Schedule` 쓰기만 구현. 읽기 경로는 아무도 사용 안 함 | `dayindex TestCloseoutDriftAndIdempotent`, `TestCatchUp` (쓰기만 검증) | partial |
| 4.2 FD 점검, LRU 풀 | `worker.open` (64/worker) | 없음 (eviction 테스트 없음, ulimit 점검 없음) | partial |
| 5.2 L_d 알고리즘, 조회량 <= m·n | `plan`, `readDayTx` | `TestPlan`, `TestRowsReadBound` | OK |
| 5.3 시간 캔들 롤업 | `query.Time` | `TestTimeAgainstBruteForce` | OK |
| 5.4 캐시 (과거/당일/링버퍼) | 없음 (architecture.md도 "no caching") | 없음 | missing |
| 6.1 단일 writer + 트랜잭션 (ticks, day_stats, candle_1m) | `worker.write` (`BEGIN IMMEDIATE`...`COMMIT`) | `TestIngestOverTCP`(정상 경로). 중간 실패 시 부분 반영 없음을 확인하는 테스트 없음 (`TestCommitFailureRetriesThenFatal`은 MkdirAll 실패라 tx 이전) | partial |
| 6.1 `last_insert_rowid()` 순서 | `upsertStats`가 ticks insert 직후 | `TestIngestOverTCP`가 last_raw_seq 검증하는지는 미확인 (unconfirmed) | partial |
| 6.2 단일 읽기 트랜잭션 + `raw_seq <= last` | `query.readTx/readDayTx`, `stream.readDay` | `TestConcurrentWriter` (쓰기 중 읽기). 상한 제거 시 실패한다는 보장은 약함 | partial |
| 6.3 과거 재판정 (같은 tx, valid_count, candle_1m, day_index 덮어쓰기) | `rejudge.rejudgeFile`, `dayindex.Set` | `TestRejudgeRestoresFreshIngest`, `TestRejudgeStricterRule`, `TestRejudgeRefusesTodayAndFuture` | OK |
| 6.3 n-tick 캐시 무효화 | 캐시 부재로 해당 없음 | - | missing (5.4 종속) |
| 6.4 롤백 복구 불필요, raw_seq 이어짐 | SQLite 기본 | `TestIngestOverTCP`(kill 없음). kill -9는 `e2e.sh --kill9` 수동만, 자동 테스트 없음 | partial |
| 6.4 장 마감 드리프트 탐지 + 재동기화 | `dayindex.checkFile`, `closeout` | `TestCloseoutDriftAndIdempotent` | OK |
| 5.5 WS snapshot/update/complete/invalidate | `stream/*` | `TestIntegration`, `TestTimeInvalidate` | OK |
| 7 요청 상한 m<=1000, n<=10000, 30일 | `query.MaxM/MaxN/MaxDays`, `api`, `stream` | `TestBadRequests`, `TestLimits`, `TestMaxDays` | OK (동시 요청 상한은 별개, D-4) |
| 7 PRAGMA 3종 | `store.Pragmas` (연결 생성 시 1회 Exec) | `TestSQLite` | partial (D-7) |

## 2. 결함과 위험 (우선순위순)

### High

**D-1. 피드 전문의 symbol 미검증으로 경로 이탈** (`wire/wire.go:Decode` 65행, `ingest/worker.go:open` 210행)
`Decode`는 symbol을 trim만 하고 `ErrSymbol` 검증을 하지 않는다(Encode만 검증). 전문의 symbol이 `../../x` 이면 `filepath.Join(dir, date, "../../x.db")`가 `dir` 밖이 되어 `MkdirAll` + 파일 생성 + 스키마 쓰기가 일어난다. 빈 symbol(".db")과 NUL/비출력 문자도 그대로 통과한다. REST/WS는 정규식을 쓰지만 신뢰 경계인 TCP 수신 쪽이 비어 있다.
수정: `Decode`에서 길이 1..12, `0x21..0x7E`, 아니면 `ErrSymbol`을 반환하고 ingest의 `read`가 이를 skip 로그 처리한다. 또는 `ingest.Put` 입구에서 공용 `symbolRe`로 검증한다. 테스트 추가 필요 (`TestBadInput`에 `../` 케이스 없음).

**D-2. 비정상 ts가 임의의 일자 디렉터리/파일을 생성** (`ingest/worker.go:split` 112~115행)
ts는 범위 검사가 없다. ts=0, 음수, int64 최댓값이면 `1970...` 또는 연도 자릿수가 비정상인 디렉터리가 생기고, 종목 x 일자 조합마다 파일이 열려 LRU를 흔든다. 같은 날짜 변조 한 건이 `day_index`/조회 대상 30일 창 안에 들어와 정상 일자를 밀어낼 수도 있다 (`listDays`는 디렉터리 이름 정렬만 사용). 수정: 수신 시각 기준 +-N일 밖의 ts는 skip + 카운터 로그(설정값), 또는 최소한 연도 1990..2100 범위.

### Medium

**D-3. FD 한도 초과 가능, LRU 스래싱** (`ingest/ingest.go:40` NumCPU worker, `worker.go:20` 64/worker)
worker 수 x 64 x 3 FD이다. 16코어면 3,072 FD로, 기본 `ulimit -n 1024`를 넘긴다 (EMFILE 시 `open` 실패 -> 재시도 -> `fatal`로 프로세스 종료). 또 종목이 worker당 64개를 넘으면(종목 수 미정, 동적 처리 가정) 매 배치마다 close/open이 반복되고, WAL 마지막 연결 close는 체크포인트와 `-wal` 삭제를 수반해 처리량이 급락한다. 읽기 요청은 요청마다 `sql.Open`/Close(파일 3개)라 별도로 FD를 쓴다. 수정: 시작 시 `RLIMIT_NOFILE` 점검 후 `maxOpenFiles = min(64, limit*0.6/(3*workers))`, 풀 크기를 설정화. 실측 필요 (unconfirmed: 실제 종목 수 N에서의 처리량).

**D-4. 요청당 최대 10M 행 스캔, 동시 요청/구독자 무제한** (`query.go:readDayTx` 136행, `api.go`, `stream/handler.go`, `cmd/ntick/main.go` 41행)
n=10000, m=1000이면 LIMIT 합계가 최대 10,000,000이다. 메모리는 스트리밍이라 출력 캔들(<=1000)만 잡히지만 CPU/IO가 크고, 동시성 제한이 없다. `readTx`는 `context.Background()`라 클라이언트가 끊어도 계속 돈다. `http.Server`에 `ReadHeaderTimeout` 등 타임아웃이 없다. WS는 구독자 수 상한이 없고 구독자당 이벤트 버퍼 256개(각 이벤트가 최대 500 tick)를 잡아 핫 종목에 수백 연결이면 수 GB가 될 수 있다 (추정: 256x500x32B ~ 4MB/구독자, 계산만 하고 측정은 안 함). 스냅샷(DB 읽기)이 Upgrade 이전에 수행되므로 연결 폭주 시 DB 읽기도 폭주한다. 수정: 요청 context 전달(`QueryContext`), 세마포어(동시 조회 N개), WS 구독자 상한, `ReadHeaderTimeout`/`MaxHeaderBytes`.

**D-5. 시간 캔들만 타임존 하드코딩** (`query/time.go:59`)
ingest와 stream은 `-tz`를 쓰는데 `query.Time`은 `Asia/Seoul` 고정이다. `-tz`를 바꾸면 REST 시간 캔들만 버킷이 어긋난다(무증상). 수정: `Time(..., loc)` 인자로 받는다 (`api.Handler`에 loc 전달).

**D-6. 한 일자 파일의 드리프트가 조회 전체를 500으로 만든다** (`query.go:167` `short read`)
`valid_count`가 실제 유효 tick 수보다 크면(수동 개입, 버그, 재판정 중간 실패 등) 해당 일자에서 에러가 나고 `NTick`이 전체 실패한다. 마감 점검이 탐지는 하지만 서비스는 계속 500이다. 수정: 일자 단위 에러는 로그 + 해당 일자 건너뛰기 또는 명시적 응답. 정책은 PO 결정 사항이므로 최소한 현상으로 보고한다.

**D-7. PRAGMA가 첫 연결에만 적용** (`store/store.go:28`)
`Create`는 `db.Exec(Pragmas)`를 한 번 실행한다. `MaxOpenConns(1)`이라도 연결이 오류 후 재생성되면(`driver.ErrBadConn`) `busy_timeout`/`synchronous`가 기본값으로 돌아간다 (`busy_timeout=0`이면 재판정/closeout과 충돌 시 즉시 SQLITE_BUSY). 읽기 쪽은 DSN `_pragma=`를 쓰므로 일관되지 않다. 수정: DSN에 `_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)`. 재현은 하지 않았다 (unconfirmed).

**D-8. 쓰기 실패 시 프로세스 종료, 미커밋 데이터 일괄 유실** (`worker.go:54,96`)
2회 실패하면 `log.Fatal`로 즉시 종료하므로 다른 worker의 큐(<=4096건 + 배치 500건)도 flush 없이 사라진다. 디스크 가득 같은 일시 장애에서 데이터 정합성은 지켜지지만(롤백) 가용성과 유실량이 크다. 수정(선택): 전체 종료 전에 다른 worker flush 시도, 또는 종료 코드/알림만 명확히. 현재 정책(정합성 우선, 폐기보다 종료)은 의도된 결정으로 보이므로 유지해도 된다. 심각도는 운영 정책에 따라 조정.

### Low

**D-9. `day_index`를 읽는 곳이 없다** (`query.listDays`, `stream.snapshot`)
PRD 5.2 1단계는 `day_index`로 일자를 고르지만 구현은 `ReadDir` + `Stat` + 일자 파일 `day_stats` 조회다. 결과는 맞지만 `day_index`는 쓰기 전용 캐시가 되었고, 빈 날도 파일을 열어야 한다 (최대 30개). 쓸지 지울지 결정 필요 (3장 참조).

**D-10. 마감 이후 늦은 tick/일자 경계 후행 tick이 `day_index`를 낡게 한다** (`dayindex.Schedule`, `ingest.split`)
20:00 closeout 이후 같은 날 도착한 tick(또는 어제 날짜로 역전된 tick)이 `valid_count`를 바꿔도 `day_index`는 다음 CatchUp까지 낡은 값이다. 읽기 경로가 안 쓰므로 현재 영향은 없다 (D-9와 함께 판단).

**D-11. 잘못된 전문의 연속은 로그 폭주** (`ingest.go:120`)
type/version 오류 전문마다 로그 한 줄을 남긴다. 스트림이 어긋나면(정렬이 깨지면) 복구 수단 없이 초당 수만 줄이다. 수정: 레이트 제한(예: 100건마다 1줄) 또는 연속 N건 오류면 재연결.

**D-12. ntickctl 인자 검증** (`dayindex.Closeout(dataDir, date)`, `cmd/ntickctl/main.go:41`)
`closeout -date ../x`는 `Glob(filepath.Join(dataDir, date, "*.db"))`에 그대로 들어간다. `rejudge`는 날짜를 파싱하지만 symbol 검증은 `/ \`만 막고 glob 메타문자(`*`, `[`)는 통과한다. 로컬 관리 도구라 위험은 낮다. 수정: `dateRe` 검증 한 줄.

**D-13. 읽기 전용 연결과 WAL** (`query.go:readTx`, `stream/snapshot.go:readDay`, `dayindex.checkFile`)
`mode=ro` 연결이 WAL 파일을 읽으려면 `-shm`이 존재하거나 생성 가능해야 한다. 정상 종료 후(마지막 연결 close로 `-wal`/`-shm` 삭제) 과거 파일을 읽는 경로에서 실제로 문제가 없는지 직접 확인하지 못했다. 읽기 테스트는 모두 통과하므로 현재 환경에서는 동작한다 (unconfirmed: 디렉터리가 읽기 전용인 운영 환경). 4.2의 "과거 파일 rollback journal 전환"을 하면 영향이 달라진다. 읽기 트랜잭션은 짧고 `readTx`는 요청마다 닫으므로 체크포인트 차단 위험은 낮다.

**D-14. 일자별 tick 순서 역전이 어제 파일에 쓰는 것은 의도이나 재판정과 경합 가능** (`rejudge.go`)
재판정은 `BEGIN IMMEDIATE` + busy_timeout(5s) (store.Create 경유)로 writer와 직렬화되고 결과는 일관된다. 다만 writer가 어제 파일의 풀 연결을 계속 들고 있으면 5초 이상 대기 시 재판정이 실패한다 (실패는 에러로 보고됨). 위험 낮음.

### 확인했고 문제 없음

- 종료 순서: `Serve` 반환 -> HTTP shutdown -> `in.Close()`가 채널을 닫고 버퍼된 tick까지 flush한 뒤 `broker.Close()` (마지막 이벤트가 구독자에게 전달됨). `TestIngestOverTCP`가 `Close` flush를 검증.
- LRU가 대기 중 쓰기를 닫는 문제: `write`가 conn을 반납한 뒤에만 `open`이 호출되고 worker 단일 goroutine이므로, eviction 시점에 해당 풀 항목의 미커밋 쓰기는 없다.
- `last_insert_rowid()` 순서, `COMMIT` 실패 시 deferred `ROLLBACK`, 재시도는 정상. `evTicks`/`Event.T`는 COMMIT 전 같은 tx에서 읽어 일관된다.
- 배치의 일자 분할: 그룹별 독립 tx이며 한 그룹 실패 시 `fatal`. 앞 그룹 커밋은 유지되고 순서는 보존된다 (`TestSplitByDate`).
- Broker: `Publish` 비차단, 락 순서 단일(`b.mu`), `Subscribe`/`Unsubscribe`/`Close` 의 `wg` 균형 확인. 슬로우 구독자는 1013으로 끊김 (`TestBrokerSlowSubscriber`).
- n-tick 산술(`plan`, `r_d`, `L_d`)과 일자 경계 분 계산(`minute = (ts-midnight)/60000`, 자정 경계)은 코드상 맞고 oracle 대조 테스트가 있다. DST가 있는 `-tz`에서는 분이 1439를 넘을 수 있으나 PRD가 Asia/Seoul로 확정이라 범위 밖.

## 3. 과잉 설계와 중복 (손댈 가치 있는 것만)

| # | 내용 | 조치 |
| --- | --- | --- |
| O-1 | `symbolRe`가 `api/api.go`와 `stream/handler.go`에 동일 복사, 검증 로직(n, interval)도 `api`/`stream.newMachine`에 이중화. D-1 수정 시 ingest에도 같은 규칙이 필요해 3곳이 된다 | `tick`(또는 새 소규모 위치)에 `ValidSymbol(string) bool` 하나로 합치고 세 곳이 호출 |
| O-2 | 읽기 전용 열기+tx 헬퍼가 `query.readTx`, `stream.readDay`, `dayindex.checkFile`에 3벌 (DSN 문자열 동일) | `store.OpenRO(path)`/`ReadTx` 하나로 합침. D-4(context 전달), D-7(DSN pragma) 수정도 한 곳에서 끝난다 |
| O-3 | 일자 디렉터리 스캔(`dayDir` 정규식 + ReadDir + Stat + 역정렬)이 `query.listDays`와 `stream.snapshot`에 복사, `dayindex`에 `dateRe` 또 하나 | `query.listDays`를 공개해 stream이 사용, 정규식 1개 |
| O-4 | 캔들 누적기가 `store.Candle.Add`와 `stream.agg.add`로 2벌 (주석에도 "same rule"이라고 적힘). 규칙이 바뀌면 둘이 어긋남 | `stream.agg`를 `store.Candle`로 대체 (필드명만 다름). 단 `TestTickMachineMatchesOracle`이 보호망이므로 안전 |
| O-5 | 응답 캔들 구조체: `query.Candle/TimeCandle`, `api.tickCandle/timeCandle`, `stream.tickCandle/timeCandle`, `tclient.tickC/timeC`, `oracle.Candle` 로 5~6벌. `api`는 `tickCandle(c)` 형변환으로 필드 순서에 묶여 있어 필드 추가 시 컴파일 에러로만 잡힌다 | `query` 구조체에 JSON 태그를 붙이고 `api`가 직접 직렬화(변환 코드와 `api` 구조체 삭제). `stream`은 `Index` 필드 때문에 별도 유지 가능 |
| O-6 | `day_index`(`dayindex.Open/Set/Closeout/Schedule/CatchUp`)가 쓰기 전용이라 현재 소비자가 없음 (D-9) | 결정: (a) `query.listDays`가 `day_index`를 읽도록 연결(PRD 5.2와 일치), 또는 (b) PRD를 "일자 스캔"으로 고치고 `day_index` 삭제. 드리프트 점검(`checkFile`)은 어느 쪽이든 유지 |
| O-7 | `query.rowsRead` 전역 atomic 카운터가 프로덕션 코드 경로에서 매 행 증가 (테스트 전용) | 원하면 테스트용 훅으로 이동. 이득이 작아 우선순위 낮음 |
| O-8 | `ingest.worker.pool`(map)+`lru`(list)+`poolEntry` 자체 구현 | 64개 상한 LRU라 직접 구현 자체는 적정. 삭제 대상 아님, D-3 수정 시 상한만 설정화 |

과잉이 아닌 것: `tick.Rule/Filter` 교체 지점(PRD 요구), `wire` 어댑터(실전문 교체 대비), `store.Candle`(ingest/rejudge 공유).
