# Sprint 관리

PO: 폐하 / PM: Claude. 기준 문서: `01.PRD.md`

## 운영 원칙

- 스프린트는 시간이 아닌 범위 단위다. 각 스프린트는 Goal, 산출물, 검증 기준(DoD)을 가진다.
- 스프린트 종료 시 PO 리뷰를 거쳐 다음 스프린트를 시작한다.
- PO 결정이 필요한 사항은 임의로 가정하지 않고 회의를 요청한다. 결정 전까지 해당 작업은 착수하지 않는다.
- 모든 진행 상황과 결정은 이 파일에 기록한다.

## 팀 구성

| 역할 | 담당 |
| --- | --- |
| PM | 스프린트 계획, 백로그, PO 소통, 결정 기록 |
| Tech Lead | 저장소 구조, 모듈 경계, 인터페이스, 코드 리뷰 |
| Ingest Dev | tick 수집, 종목별 writer, 트랜잭션, 1m 집계 |
| Query Dev | n-tick 조회, 시간 캔들 롤업, 스냅샷 읽기, 캐시 |
| API/Realtime Dev | REST, WebSocket, 이벤트 발행·`invalidate` |
| Mock Feed Dev | 시세 발생기 목업 (정상/invalid/역전/정정/재연결 시나리오) |
| QA / Test Client | 테스트 클라이언트, 전수 비교용 oracle, 장애 주입 테스트 |

## Git 운영 원칙

PM이 Tech Lead·QA 의견을 받아 정했다. 작업 트리를 모든 팀원이 공유하므로 충돌 방지가 핵심이다.

- 실행 주체 (v2, PO 지시 2026-10-02에 따라 개정): PM은 `main`의 병합·태그·게이트를 맡는다. 팀원 에이전트는 PM이 만들어 준 자신의 worktree와 브랜치 안에서만 `add`/`commit`을 실행한다. `checkout`, `stash`, `reset`, `rebase`, `merge`, `push`, 강제 옵션, 다른 worktree 접근은 금지한다(`push`는 PM만 실행한다). 전역 지침(직접 git 실행 금지)의 이 프로젝트 한정 예외다. 적용 시점은 `baseline` 커밋 이후의 작업부터이며, 그 전 작업은 공유 작업 트리에서 진행되었다.
- 브랜치와 태그: `main`은 항상 게이트를 통과한 상태로 둔다. 팀원 브랜치는 `<역할>/s<N>-<주제>`(예: `ingest/s6-cache`), worktree는 `../ntick-wt/<역할>`이며 PM이 `main`에서 만들어 준다. 작업이 끝나면 PM이 diff를 검토(시험을 약화시킨 변경 여부 포함)하고 의존 순서대로 `--no-ff`로 병합한다. 승인된 스프린트마다 태그 `sprint-N`, 초기 이관은 `baseline`.
- 원격 동기화(PO 승인 2026-10-02, 이 프로젝트 한정): `origin`은 `https://github.com/yonghee-mirae/ntick.git`. PO가 다른 PC에서 결과물을 확인하므로 원격을 항상 최신으로 충실히 유지한다. PM이 다음 시점에 push한다: ① `main` 병합 직후 ② 태그 생성 직후(`git push origin <tag>`로 명시) ③ 팀원 브랜치 작업이 끝나 병합 검토를 시작할 때(브랜치 push) ④ 스프린트 보고 직전. 결과물 보고는 push 완료 후에만 한다. push 후 `git fetch origin`과 `git status -sb`로 ahead/behind가 0인지, `git ls-remote origin`의 `main`·태그 SHA가 로컬과 같은지 확인하고 PO 보고에 커밋 SHA를 적는다. push 전에는 `git status -uall`과 비밀값 점검을 한다.
- 원격 금지 사항: force push, 원격 히스토리 재작성, 원격 브랜치·태그 삭제(PO가 요청한 경우 제외). 인증 실패나 원격 거부가 나면 우회하지 않고 PO에게 알린다.
- 커밋 단위: 한 커밋 한 목적. 제품 코드는 패키지(담당 영역)별, 문서는 코드와 분리한다. 시험 기대값·하네스(`oracle`, `tclient`, `scripts`)는 제품 코드와 분리해 커밋하고 변경 이유를 적는다(시험을 약화시킨 변경이 섞이는 것을 막는다).
- 메시지: Conventional Commits, scope는 패키지명, 영어 제목 72자 이내, 본문에 what/why와 통과한 검증 요약 한 줄, 하네스가 요구하는 트레일러. 환경 의존 수치는 커밋이 아니라 `docs/perf-report.md`에 둔다.
- 커밋 게이트: 모든 팀원 idle 확인 → `gofmt -l .` 비어 있음 → `go mod tidy` 후 diff 없음 → `go vet ./...` → `go test -count=1 ./...`. ingest, query, store, stream, 스키마를 건드린 커밋은 `scripts/e2e.sh` 1회 추가. 게이트 직후와 커밋 직전의 `git status`/diff가 같은지 확인한다.
- 태그 게이트(`sprint-N`): PO 승인 후, 다른 작업을 멈춘 상태에서 `go test -race ./...`, `e2e.sh` 3회, `e2e.sh --kill9` 1회, `fault.sh all` 1회(약 6~7분). 출력 마지막 줄(`E2E PASS`, `RESULT ... PASS`, `0 mismatches`)까지 확인한다.
- 커밋 금지: `data/`, `*.db*`, 바이너리, `truth.csv`, 성능 시험 임시 데이터, 비밀값. 커밋 직전 `git status -uall`로 새로 추적될 파일을 눈으로 확인한다. 의존성 추가는 Tech Lead만 하며 `go.mod`/`go.sum`은 커밋하고 vendoring은 쓰지 않는다.
- 하네스(`scripts`, `tclient`) 변경은 변조 시험(mutation) 1회로 실제 불일치를 잡는지 확인한 뒤 커밋한다.
- 위험과 완화: worktree로 공유 트리의 간섭(반쯤 수정된 파일, 서로의 시험 방해)은 사라지지만 병합 충돌이 생긴다. 파일 소유 구역을 유지하고, `go.mod`/`go.sum`은 Tech Lead 브랜치에서만 바꾸며, 공유 파일(`cmd/ntick/main.go`, 문서)은 한 스프린트에 한 브랜치만 수정한다. 충돌은 PM이 병합 때 해결하고 병합 후 `main`에서 게이트를 다시 통과시킨다. 성능 측정은 다른 브랜치 작업이 없는 조용한 시간에만 한다. 각 팀원의 완료 보고는 자기 브랜치에서 `go vet`·`go test`를 통과시킨 뒤에 한다.

## 스프린트 로드맵 (초안)

| Sprint | Goal | 검증 기준 |
| --- | --- | --- |
| 0 | 결정 사항 확정, 저장소 골격, 목업 피드·tick 인터페이스 정의 | PO 결정 기록, 빈 파이프라인 end-to-end 기동 |
| 1 | 저장·수집: 스키마, 종목별 writer, 1m 집계, 목업 피드 연동 | 강제 종료 후 `COUNT(valid=1) == valid_count`, 1m 집계가 oracle과 일치 |
| 2 | n-tick 조회: 알고리즘, 스냅샷 읽기, 테스트 클라이언트(조회) | invalid 혼재·일자 경계·m 초과 케이스가 brute-force oracle과 일치, 조회량 ≤ m·n |
| 3 | 시간 캔들 롤업, REST API | interval별 롤업이 oracle과 일치, `ts` 역전 케이스 통과 |
| 4 | 실시간 푸시(WS), 사후 invalid 정정, `day_index` 동기화, 캐시 | 정정 후 카운터·캔들·`day_index` 일치, `invalidate` 수신 확인 |
| 5 | 안정화, 부하·장애 시험, 비기능 목표치 실측 보고 | 실측 결과를 PO에 제출해 7장 미정 목표치 확정 |

## 결정 대기 (PO 회의 필요)

| # | 안건 | 상태 | 막는 작업 |
| --- | --- | --- | --- |
| D1 | 구현 언어·런타임 | 확정: Go (PO가 PM·팀에 위임) | - |
| D2 | 목업 시세 발생기의 전문 형식과 전송 방식 | 확정: 바이너리 고정 길이 전문, TCP 스트림 (PO 승인). 레이아웃은 목업 가정이며 어댑터로 격리 | - |
| D3 | invalid 판정 방식 | 확정(정정됨): 별도 판정 채널 없음. 수신·기록 시스템에 통합된 규칙 기반 수신 필터(기준은 설계 단계에서 정하고 쉽게 교체 가능한 구조). 이후 변경은 최소 하루가 지난 일자의 일괄 재판정만. PRD 반영 완료 | - |
| D4 | 테스트 클라이언트의 형태 | 확정: CLI + 자동 검증 | - |
| D5 | 소스의 재전송과 중복 식별 | 확정: 재전송 없음, 끊긴 구간 누락만 허용, 중복 제거 안 함. 소스의 종목 내 순서 보장 여부는 미확인(n-tick 정확성의 전제, PRD 8장 유지) | - |
| D6 | 거래소 타임존, 종목 규모, 보관 기간 | 확정: 타임존 Asia/Seoul, 종목 수는 사전 정의 없이 수신 전문의 종목코드로 동적 처리. 보관 기간은 미정(Sprint 1을 막지 않음) | - |
| D7 | API 응답 JSON 스키마 | Sprint 2 전에 요청 | Sprint 2~3 |

## 스프린트 기록

### Sprint 0 — 준비 (완료, PO 승인 2026-10-02)

- Backlog
  - [x] 저장소 골격, `docs/architecture.md` (Tech Lead, PM 검증: `go vet`, `go test -count=1 ./...` 통과)
  - [x] `internal/tick`: `Tick`, 교체 가능한 `Rule`/`Filter`, 기본 규칙 price>0 && qty>0 (Tech Lead)
  - [x] SQLite 드라이버 스파이크, PRD 6.1 SQL 실행 검증 (Tech Lead): modernc.org/sqlite 선택
  - [x] 목업 시세 발생기 사양서 `docs/mock-feed-spec.md` (Mock Feed Dev, PM 검증: vet·test 통과)
  - [x] 전문 인코더·디코더 `internal/wire`, 발생기 `cmd/mockfeed` (Mock Feed Dev, PM 검증: vet·test 통과)
  - [ ] 빈 파이프라인(목업 → 수집 → 파일) 기동 (Tech Lead 완료 후)
- 산출 요약
  - 목업 전문: 38바이트 빅엔디안(type, version, symbol 12B, ts, price, qty), 구분자·길이 필드 없음. 실제 전문과 다를 수 있는 목업 가정
  - 발생기: 시드 기반 결정적, 이상 tick·ts 역전·중복·다일자·연결 끊김 시나리오, 정답 파일(CSV `idx,symbol,ts,price,qty,label`)
  - 한계: 재전송은 구현하지 않음(D5 미결), Asia/Seoul·09:00 시작은 임의 기본값(D6 미결)
  - 드라이버: modernc.org/sqlite(pure Go, cgo 불필요). 번들 SQLite 3.53.4(요구 3.25 이상 충족). 동일 테스트에서 mattn 대비 약 1.6배 느림(34.5k vs 56.2k tick/s, 단일 PC, 미최적화 조건). 처리량 목표 확정 후 Sprint 5 실측에서 필요 시 교체
  - PRD 6.1 SQL은 그대로 동작. 단 `last_insert_rowid()`는 `ticks` insert 직후 `day_stats`에서 읽는 순서를 지켜야 함(반대 순서는 미시험)
  - `candle_1m`의 open/close 교체: 새 tick은 raw_seq가 가장 크므로 open은 ts가 엄격히 작을 때, close는 ts가 같거나 클 때 교체(테스트 확인)
  - invalid 규칙은 기본값만 구현. 임계값은 임의로 만들지 않음(PO 확인 대기)

### Sprint 1 — 저장·수집 (완료, PO 승인 2026-10-02)

- Goal: 목업 피드 → 수신 필터 → 종목별 writer → 일자·종목 SQLite 파일, 1m 집계, 정합성 검증
- DoD: 강제 종료(kill -9) 후 모든 파일에서 `COUNT(valid=1) == valid_count`, `last_raw_seq == max(raw_seq)`, `SUM(candle_1m.tick_count) == valid_count`. 1m 캔들이 정답 파일(truth) 기반 oracle과 일치
- 담당
  - Ingest Dev: `internal/ingest`, `cmd/ntick`, `internal/store` 쓰기 API (TCP 수신, 어댑터, 필터, 해시 고정 worker, micro-batch 커밋, 파일 LRU 풀, 재연결)
  - QA / Test Client: `cmd/tclient`, `internal/oracle` (truth 기반 기대값 계산, DB 읽기 검증, 정합성 점검)
- Backlog
  - [x] 수집 서버 `internal/ingest`, `cmd/ntick` (Ingest Dev). PRD 6.1 트랜잭션, worker 고정 배정(fnv32a, CPU 수), micro-batch(500건/50ms), worker별 LRU 풀(64), 백오프 재접속, 종료 시 flush
  - [x] 통합 시험 (PM 직접 실행): `go vet`, `go test -race ./...` 통과, e2e 3회 PASS(truth 6,287행, 15파일, 불일치 0), kill -9 모드 3회 PASS(정합성 3종)
- 한계·미결 (PO 결정 필요)
  - 커밋 실패 시 배치를 로그만 남기고 폐기함. 정책 확정 필요(폐기 / 재시도 / 프로세스 종료)
  - 잘못된 type·version 전문은 skip과 로그
  - 수신 처리량은 미측정(Sprint 5), 커밋 이벤트 발행은 Sprint 4 범위
  - kill -9 모드는 전송 중 유실 때문에 정확성 비교(verify)를 하지 않고 정합성 3종만 검증
  - [x] 정합성 점검·oracle 검증 CLI (QA 완료 보고, e2e 일반·kill9 각 1회 PASS 보고. PM 재검증은 Ingest 완료 후)
  - [ ] 통합 시험: 정상, 이상 tick, ts 역전, 중복, 다일자, 연결 끊김, kill -9 (PM 주관)

### Sprint 2 — n-tick 조회 (완료, PO 승인 2026-10-02)

- Goal: PRD 5.2 알고리즘을 라이브러리(`internal/query`)로 구현하고 oracle과 전수 비교
- DoD: 임의 (n, m)에서 결과가 oracle `NTick`과 일치, 조회 tick 수 ≤ m·n, 상한(m ≤ 1,000, n ≤ 10,000, 30일) 초과는 오류, 수집 중 읽어도 스냅샷 일관성 유지(T_d와 tick 조회가 한 읽기 트랜잭션)
- 담당
  - Query Dev: `internal/query` (n-tick 조회, 스냅샷 읽기, 읽기 전용 연결)
  - Ingest Dev: 커밋 실패 정책 반영(재시도 1회 후 프로세스 종료)
  - QA: Query 완료 후 `tclient verify-ntick` 추가, e2e 확장
- PM 설계 판단 (PRD와의 차이, 리뷰 시 확인)
  - 과거 일자 T_d는 `day_index`(meta.db)가 아니라 각 일자 파일의 `day_stats`에서 읽는다. PRD 4.1이 `day_index`를 `day_stats`로 재생성 가능한 캐시로 정의하므로 결과는 같다. `day_index` 기록은 장 마감 시각 정의가 필요해 Sprint 4로 미룬다(PO 결정 필요 항목)
- Backlog
  - [x] n-tick 조회 `internal/query` (Query Dev, PM 검증: `go vet`, `go test -race ./...` 통과, e2e·kill9 PASS). `NTick(dataDir, symbol, n, m)`, 최신순, 읽기 전용 단일 읽기 트랜잭션. 시험: oracle 무작위 300회 일치, 읽은 행 ≤ m·n 확인, 수집 중 동시 읽기 40회 일관성
  - 미결 (PO 결정 필요): ① "최대 30일"을 유효 tick 일자가 아닌 파일 30개 기준으로 셈 ② 읽기 실패 일자(손상 등)를 건너뛸지 전체 오류로 할지(현재 전체 오류) ③ 과거 파일을 rollback journal 모드로 전환하거나 읽기 전용 매체에 둘 때의 `mode=ro` 영향(현재 WAL만 시험)
  - [x] 커밋 실패 정책 (Ingest Dev 완료 보고: 재시도 1회 후 `log.Fatal`, 종료 코드 1. 다른 worker의 대기 배치는 flush하지 않음 = kill과 동일한 의미. PM 재검증은 Query 완료 후)
  - [x] `tclient verify-ntick` (QA, PM 검증: e2e에서 5종목 300케이스 불일치 0, 상한 초과 음성 검사 통과). 한계: 시험 데이터가 3일치라 30일 상한 경로는 e2e로 미검증(query 단위 시험에서는 검증됨)

### Sprint 3 — 시간 캔들, REST API (완료, PO 승인 2026-10-02, 후속 조치 진행 중)

- Goal: `candle_1m` 롤업 시간 캔들 조회와 REST API(`GET /candles` time·tick), oracle 독립 검증
- DoD: 임의 interval·범위의 시간 캔들이 raw tick에서 직접 계산한 oracle과 일치(candle_1m 집계와 롤업 모두 검증), REST 응답이 `query` 결과와 일치, 잘못된 파라미터는 HTTP 400
- 담당
  - Query Dev: `internal/query`에 시간 캔들 조회 추가
  - API Dev: `internal/api`(HTTP 핸들러), `cmd/ntick`에 `-http` 연결 (수집 서버와 한 프로세스)
  - QA: oracle에 시간 캔들 기대값 추가 후, `verify-time`·`verify-api` 추가
- 계약(팀 공통)
  - `query.Time(dataDir, symbol string, intervalMin int, from, to int64) ([]query.TimeCandle, error)`, `query.ParseInterval(s) (int, error)` (m/h/d)
  - `TimeCandle{Start int64 (epoch ms), Open, High, Low, Close, Volume int64, TickCount int, Partial bool}`, 시간 오름차순, [from, to)
  - 일자 파일 단위로 현지 자정 기준 버킷 정렬, 버킷은 자정을 넘지 않음, 틱 없는 구간은 생략(FR-5a)
  - REST: `GET /candles?symbol=&type=time&interval=&from=&to=` (from/to epoch ms), `GET /candles?symbol=&type=tick&n=&m=` (최신순). JSON, 가격·수량은 정수 최소단위, 시각은 epoch ms. 오류는 HTTP 400/500과 `{"error": "..."}`
- PM 가정 (리뷰 시 확인): 시간 캔들 조회에도 n-tick과 같은 최대 30일(파일 30개) 상한을 적용한다. PRD에 시간 캔들 조회 상한이 없어 보수적으로 둔 값이다
- Backlog
  - [x] 시간 캔들 조회 `internal/query/time.go` (Query Dev). 버킷은 현지 자정 정렬·자정 미초과, 미완성은 데이터 시각 기준(종목 전체 유효 최대 ts), brute-force 300회 일치
  - [x] REST API `internal/api`, `cmd/ntick -http` (API Dev). snake_case JSON, 파라미터 오류 400, 읽기 실패 500, symbol 정규식 검증(경로 조작 차단), SIGTERM 시 graceful shutdown
  - [x] oracle 시간 캔들(raw tick 독립 계산), `verify-time`, `verify-api` (QA)
  - PM 검증: `go vet`, `go test -race ./internal/...` 통과, e2e 2회 PASS + kill9 PASS (verify 5종목 불일치 0, verify-ntick 300케이스, verify-time 151케이스, verify-api 433케이스 보고). 변조 시험(ticks 가격 1건, candle_1m 고가 1건 변조)에서 verify·verify-ntick·verify-time 모두 불일치 탐지 확인
  - PM 조치 예정(PO 결정 불필요): 405 응답 본문을 JSON `{"error":...}`로 통일
  - 미결 (PO 결정 필요): ① 시간 캔들 범위가 최신 30개 파일을 넘을 때(현재 조용히 잘림) ② 데이터 디렉터리·종목 없음 응답(현재 디렉터리 없음 500, 종목 없음 200 + 빈 목록) ③ `day_index` 확정 시점(Sprint 4)
  - 한계: 시험 데이터는 3일치라 30일 상한 경로는 e2e 미검증(query 단위 시험에서는 검증됨), 인증·CORS·요청 타임아웃 없음

### Sprint 4 — 실시간 푸시, 장 마감 확정, 과거 일자 재판정 (완료, PO 승인 2026-10-02)

- Goal: WebSocket 실시간 갱신, 장 마감 시 `day_index` 확정과 정합성 점검, 최소 하루가 지난 일자의 일괄 재판정
- DoD: 스트림으로 받은 `complete` 캔들 열이 oracle과 일치(tick·time), 연결 시 snapshot 이후 누락·중복 없음, 장 마감 점검이 드리프트를 탐지, 재판정이 변조된 `valid`를 복구하고 `valid_count`·`candle_1m`·`day_index`가 일치
- 담당
  - Realtime Dev: `internal/stream`(브로커, WS 핸들러, 진행 중 캔들 상태), ingest 커밋 후 이벤트 훅, `cmd/ntick` WS 연결
  - Ingest Dev: `internal/dayindex`(meta.db `day_index`, 장 마감 점검), `internal/rejudge`, `cmd/ntickctl`(`closeout`, `rejudge`). `cmd/ntick` 연결은 Realtime 완료 후
  - QA: Realtime·Ingest 완료 후 `verify-stream`, 재판정 복구 시험을 e2e에 추가
- PM 해석·범위 조정 (리뷰 시 확인)
  - 캐시(PRD 5.4)는 Sprint 5 실측 후 필요 시 도입 (결정 기록 참조)
  - `day_index`는 확정 기록과 점검 용도다. n-tick 조회의 T_d 판정은 계속 일자 파일의 `day_stats`(스냅샷 일관성)를 쓴다. 질의 경로에서 `day_index`를 쓰는 것은 실측 후(Sprint 5)
  - 재판정 대상은 "현지 날짜가 오늘보다 이전인 일자"로 해석한다. 재판정은 현재 컴파일된 수신 필터 규칙을 raw_seq 순서로 다시 적용한다
  - PRD 6.4의 `COUNT(*)` 비교는 `COUNT(valid=1)`의 오기로 보고 정정했다(`valid_count`가 유효 tick 수이므로)
- Backlog
  - [x] WS 실시간 `internal/stream`, ingest 커밋 훅(`OnCommit`), `cmd/ntick` 연결 (Realtime Dev, PM 검증: vet, 전 패키지 `-race`, e2e, kill9 통과). 의존성 `github.com/coder/websocket v1.8.15`(순수 Go). snapshot 이후 이벤트만으로 상태 갱신, 이미 snapshot에 포함된 이벤트는 raw_seq로 제거, 느린 구독자(버퍼 256 이벤트 초과)는 close code 1013으로 끊음. tick 캔들 스트림에 `index`(일자 내 0부터 순번) 필드 추가
  - Realtime 해석 (PM 판단, 리뷰 시 확인): tick `complete`는 T가 n의 배수가 되는 즉시 전송(PRD 5.5 문구대로, 그 시점 접속자의 snapshot은 null), 진행 중인 날 이전 일자의 늦은 tick은 tick 스트림에서 무시(자정 break time 가정), 구독자 수 상한은 두지 않고 Sprint 5 부하 측정 후 판단
  - [x] `day_index` 확정, 장 마감 점검 `internal/dayindex`, `cmd/ntickctl closeout` (Ingest Dev, PM 검증: `go test -race` 통과). `cmd/ntick` 연결(CatchUp, 스케줄러)은 Realtime 완료 후
  - [x] 과거 일자 재판정 `internal/rejudge`, `cmd/ntickctl rejudge` (Ingest Dev, PM 검증 동일). 양방향 변조 복구가 신규 수집 결과와 일치, dry-run·오늘 이후 날짜 거부 시험 통과
  - [x] 후속: `candle_1m` 로직을 `internal/store/candle.go`로 단일화(ingest·rejudge 공용), `cmd/ntick`에 `CatchUp`(시작 시 항상)과 `-close HH:MM`(기본 20:00, 빈 문자열이면 비활성) 연결 (Ingest Dev 완료 보고, PM 최종 검증은 QA 완료 후)
  - 한계: 마감 스케줄러가 실제 20:00에 도는 경로는 e2e로 검증하지 못했다(목업 날짜가 2026-01 고정이라 "오늘" 파일이 없음). `NextClose` 단위 시험만 있음 → Sprint 5에서 mockfeed에 시작 일자 지정을 추가해 종단 검증
  - PM 운영 결정 (PRD 범위 내, 리뷰 시 확인): 드리프트는 로그와 `ntickctl` 종료 코드 1로만 알리고 자동 복구는 하지 않는다(PRD 6.4는 탐지만 요구). 서버는 시작 시 `CatchUp`을 항상 실행하고 마감 스케줄러는 `-close HH:MM`(기본 20:00)으로 설정한다. 재판정 후 `day_index` 갱신 실패는 경고하고 다음 마감 점검에 맡긴다
  - [x] `verify-stream`, 재판정 복구 e2e (QA 완료, PM 검증)
  - PM 최종 검증: `go vet`, 전 패키지 `go test -race` 통과, e2e 2회 PASS(verify-api 433케이스, verify-stream 구독 60개 중 중간 합류 25개, verify-ntick 300케이스, verify-time 151케이스 모두 불일치 0, 재판정 복구 e2e 포함), kill9 PASS
  - 한계: 시험 데이터에서 time 스트림 `invalidate`가 실제 발생했는지 미확인, 재판정 e2e의 변조가 양방향 뒤집기를 보장하지는 않음(단위 시험에서는 양방향 검증), 마감 스케줄러 20:00 실행 경로 미검증
  - [x] 후속(Sprint 3 리뷰 결정): 데이터 없음 200, 30개 파일 초과 400(`query.ErrRangeTooLarge`), 405 JSON (API Dev 완료, PM 검증: query·api `-race` 통과). `docs/architecture.md` API 절 갱신은 미반영

### Sprint 5 — 안정화, 장애·부하 시험, 비기능 실측 (구현·측정 완료, PO 리뷰 대기)

- Goal: 코드 리뷰와 PRD 추적, 장애 주입, 마감 스케줄러 종단 검증, 비기능 실측 보고. 목표치는 실측 결과를 보고 PO가 정한다(PRD 7장)
- DoD: PRD 기능 요구사항(FR-1~FR-12)별 구현·시험 추적표, 장애 시험(반복 kill -9, 디스크 가득, FD 한도, 피드 끊김 반복, 읽기·쓰기 동시) 통과 또는 결함 보고, 마감 스케줄러가 "오늘" 일자에서 실제로 동작, 실측 보고서(`docs/perf-report.md`)와 권고(캐시, 드라이버, m·n 상한)
- 단계
  - A (병렬): Mock Feed Dev `-start-date`(오늘 일자 시뮬레이션) 추가 / Reviewer 코드·PRD 감사(읽기 전용, `docs/review.md`)
  - B: QA 장애 주입과 마감 스케줄러 종단 시험 (Reviewer 결함 반영 후)
  - C: 성능 엔지니어 실측 (다른 작업이 없는 상태에서 단독 실행, 측정 왜곡 방지)
  - D: 결함·권고 정리, README와 `docs/architecture.md` 갱신, 최종 PO 리뷰
- Backlog
  - [x] `mockfeed -start-date`, `-start-time` (Mock Feed Dev, PM 검증: vet·test 통과). 생성기 자체 처리량은 약 11M msgs/s(종목 수 무관)로 병목 아님. `-rate 0`의 실제 한계는 서버 쪽 소켓 write(메시지당 syscall 1회)와 truth 기록이라 부하 시험에서 병목이면 `bufio`로 묶는 방안이 있음
  - [x] 코드·PRD 감사 `docs/review.md` (Reviewer, PM이 상위 3건을 코드에서 직접 확인)
  - [ ] 감사 결함 수정 (진행 중)
    - [x] Ingest Dev (PM 검증: wire·ingest·store·dayindex·rejudge `-race` 통과): ① 피드 symbol·ts 검증(경로 이탈, junk 일자 디렉터리 생성 차단, 잘못된 전문은 skip과 로그로 PO 승인 정책과 동일, 로그는 초당 1줄 제한) ② FD 한도 기반 LRU 풀 크기(RLIMIT_NOFILE의 절반 예산, 4~64, 시작 시 로그) ③ PRAGMA를 모든 연결에 적용(DSN `_pragma`) ④ 6.1 "부분 반영 없음" 증명 시험(트리거로 중간 실패 유도, 롤백 확인, 이후 정상 커밋 지속). 한계: 재시도 횟수 자체는 시험이 세지 않음
    - [x] Query Dev (PM 검증: vet, 전 패키지 `-race`, e2e 2회 + kill9 PASS): ① 시간 캔들 타임존 하드코딩 제거(`-tz` 전달, Seoul·Tokyo·New_York 시험) ② 조회 context 전파(20만 행 취소가 2초 내 반환 시험) ③ http.Server `ReadHeaderTimeout` 5초, `IdleTimeout` 60초 ④ `store.OpenRO` 단일화(`dayindex.checkFile`은 남아 있음, 한 줄 교체 예정)
  - [x] B단계: QA 장애 주입 `scripts/fault.sh` (QA 보고, 시나리오별 2회 이상 실행). 신규 도구: `tclient verify-subsequence`, `tclient stress-read`
    - PASS: 마감 스케줄러 종단(오늘 일자 5개 파일, 로그·`day_index`·`ntickctl closeout` 일치)과 CatchUp(과거 15개 파일 채움), 반복 kill -9(12회, 매번 정합성 PASS, 저장 tick은 정답의 순서 부분열, 유실 약 500/62,970건은 전송 중 유실), 디스크 쓰기 실패(재시도 후 종료 코드 1, 재시작 후 정합성 PASS), FD 한도(`ulimit -n 256`, 600개 파일, EMFILE 없음, 유실 0), 피드 끊김 126회(저장 6287/6287), 읽기·쓰기·재판정 동시(요청 약 6천건 5xx 0, WS 약 3만 메시지 불변식 위반 0), 잘못된 피드 입력(junk 프레임 780개, 정상 60행만 저장, 밖으로 파일 생성 없음)
    - FAIL 1건(결함): 신규 일자 파일 생성 레이스. writer가 파일을 만든 직후 스키마 적용 전에 조회가 그 파일을 잡아 HTTP 500(`no such table`). 같은 뿌리로 디스크 쓰기 실패 중 파일 생성이 끊기면 스키마 없는 파일이 남음
    - [x] 수정 (Ingest Dev): 파일 생성을 원자적으로(임시 이름에서 스키마 적용 후 rename). PO의 읽기 실패 정책(손상 파일은 요청 전체 오류)은 그대로 유지. PM 검증: vet, 전 패키지 `-race`, e2e PASS, `fault.sh newday` PASS(요청 5,220건 5xx 0). 수정 효과는 원자화를 끄면 시험이 실패하는 것으로 확인
    - 재실행 중 발견(PM): `fault.sh all`에서 `fd` 시나리오가 4회 중 3회 FAIL(유실처럼 보임). 원인은 하네스: 피드가 끝난 시점(truth 파일 정지)에 ntick을 SIGTERM하는데, FD 한도로 ntick이 피드보다 느려 소켓 버퍼에 읽지 않은 데이터가 남아 있었다(순수 TCP 클라이언트는 6만 건 전부 수신해 mockfeed는 정상임을 확인). 제품 결함이 아니라 종료 의미(kill과 동일)이며, 하네스가 ntick이 다 소비할 때까지 기다리도록 수정 중(QA). 관찰: FD 한도(풀 4/worker)에서는 처리량이 약 8k tick/s로 낮다
    - Query Dev: ① 시간 캔들 타임존 하드코딩 제거(`-tz` 전달) ② 조회 context 전파(클라이언트 끊김 시 취소) ③ http.Server 헤더·유휴 타임아웃 ④ 읽기 전용 열기 헬퍼 단일화
    - 보류(실측 후 판단): 동시 요청 상한, WS 구독자 상한, m·n 최대 1천만 행 조회 비용, 캐시
    - PRD 정정: `day_index`는 확정 기록·점검 용도이고 조회 T_d는 `day_stats`를 읽는다(PO가 Sprint 4에서 승인한 내용에 맞춰 4.1, 4.2, 5.2 문구 수정)
    - PM 선택 상수(리뷰 시 확인): 피드 ts 허용 범위 2000-01-01 이상 2100-01-01 미만
  - [x] 장애 주입, 마감 스케줄러 종단 시험 (QA 완료, PM 재검증: vet, 전 패키지 `-race`, `FSIZE=60000 fault.sh disk` PASS, `fault.sh fd` 2회 PASS, e2e PASS(`drained: 6287 of 6287`)). 하네스 수정: 완료 판단을 "저장 행 수가 정답 행 수와 같아질 때까지(유실 허용 시나리오는 증가가 멈출 때까지)"로 변경, `tclient count-ticks` 추가. fd 한도 처리량 약 2.7k tick/s(폴링과 경합한 값이라 참고치)
  - [x] 실측 `docs/perf-report.md` (성능 엔지니어, 약 5시간 50분). 제품 코드 변경 없이 `*_bench_test.go`(`//go:build perf`), `scripts/bench.sh`, `cmd/tclient/bench.go`만 추가
    - 핵심 수치(i5-10400 6C/12T, NVMe): 수집 steady 5종목 478k, 100종목 275k, 500종목 79k, **2000종목 1.1k tick/s**(LRU pool 절벽, pool 256이면 26k). n-tick 조회 행당 약 890 ns, m·n 100 ms≈105k, 500 ms≈550k, 2 s≈2.2M, **현 상한 10M행은 9 s(warm)·16 s(cold)**. modernc는 동시 조회 2개에서 포화하고 수집을 최대 -63% 깎음(mattn은 8개에서 5.1배). 신규 일자 파일 생성 48 files/s. 실시간 commit→수신 p99 < 1 ms, 종단 p99 30~60 ms(flush 50 ms). 저장 36.8 B/틱(인덱스 제거 시 23.1), 3000종목×250일≈1.38 TB(추정)
    - PM 검토: 보고서를 끝까지 읽었고 권고(목표치 제안, 캐시 보류, 드라이버, 상수, `day_index`, 보관)를 PO 결정 항목으로 정리함. 수치는 PM이 재현하지 않았음(측정은 PM이 직접 돌리지 않음)
  - [x] README, `docs/architecture.md` 갱신 (PM)

## 결정 기록

| 일자 | 결정 | 근거 |
| --- | --- | --- |
| 2026-10-02 | `ts`는 epoch ms로 저장 | PO |
| 2026-10-02 | n-tick 정렬은 raw_seq, 시간 캔들은 ts(동률 시 raw_seq) | PO |
| 2026-10-02 | 요청 상한 m ≤ 1,000, n ≤ 10,000, 최대 조회 30일 | PO |
| 2026-10-02 | 구현 언어 Go. PO가 Go/Rust/C/C++ 선호를 밝히고 선택을 PM·팀에 위임. 종목별 writer를 goroutine으로 두기 쉽고 단일 바이너리 배포가 가능해 선택(PM 판단). SQLite 드라이버(cgo/순수 Go)는 Sprint 0에서 SQLite 버전 3.25 이상 확인과 함께 정함 | PO 위임, PM |
| 2026-10-02 | 목업 전문은 바이너리 고정 길이 | PO |
| 2026-10-02 | (정정됨) 별도 판정 채널은 두지 않는다. 수신·기록 시스템에 통합된 규칙 기반 수신 필터로 판정하며 기준은 설계 단계에서 정하고 교체 가능한 구조로 둔다 | PO |
| 2026-10-02 | invalid의 이후 변경은 최소 하루가 지난 일자 전체의 일괄 재판정만 지원한다. tick 단위 당일 정정은 없다 (PM이 PO 발언을 이렇게 해석해 PRD 6.3에 반영, 스프린트 리뷰에서 확인 필요) | PO, PM 해석 |
| 2026-10-02 | 테스트 클라이언트는 CLI + 자동 검증 | PO |
| 2026-10-02 | 소스 재전송 없음, 누락만 허용, 중복 제거 안 함 | PO |
| 2026-10-02 | 타임존 Asia/Seoul, 종목은 수신 전문의 종목코드로 동적 처리 | PO |
| 2026-10-02 | invalid 수신 필터는 기본 규칙(price>0, qty>0)만 | PO |
| 2026-10-02 | Sprint 0 승인: TCP 스트림, modernc.org/sqlite(실측 후 교체 가능), PRD 6.3 해석 | PO |
| 2026-10-02 | 커밋 실패 시 한 번 재시도 후 프로세스 종료 | PO |
| 2026-10-02 | 잘못된 type/version 전문은 skip과 로그, 피드 재접속은 백오프로 무한 반복(현재 동작 유지) | PO |
| 2026-10-02 | Sprint 1 승인: n-tick 결과는 최신 캔들 우선, kill -9 합격 기준은 정합성 3종 | PO |
| 2026-10-02 | "최대 30일"은 종목 파일 30개 기준, 읽기 실패 일자는 요청 전체 오류 | PO |
| 2026-10-02 | API 가격·수량은 정수 최소단위, 시각은 epoch ms | PO |
| 2026-10-02 | Sprint 2 승인: 과거 T_d는 각 일자 `day_stats`에서 읽음(`day_index` 기록은 Sprint 4) | PO |
| 2026-10-02 | 시간 캔들 미완성 판정은 데이터 시각 기준(구간 끝이 해당 종목의 보유 데이터 최신 ts보다 뒤) | PO |
| 2026-10-02 | 시간 캔들 조회는 시간 오름차순, from/to는 epoch ms [from, to) | PO |
| 2026-10-02 | 시간 캔들 범위가 최신 30개 파일을 넘으면 HTTP 400 | PO |
| 2026-10-02 | 데이터 디렉터리·종목이 없으면 항상 200 + 빈 candles | PO |
| 2026-10-02 | day_index는 장 마감 시각 설정값으로 확정, 시각은 20:00 (Asia/Seoul) | PO |
| 2026-10-02 | WS 메시지 형식은 PM 제안대로: snapshot/update/complete/invalidate, candle 필드는 REST와 동일 | PO |
| 2026-10-02 | Sprint 3 승인: 시간 캔들에도 30개 파일 상한 적용 | PO |
| 2026-10-02 | 캐시(PRD 5.4)는 Sprint 4에서 제외하고 Sprint 5 실측 후 필요 시 도입 | PM 제안, PO 승인 |
| 2026-10-02 | Sprint 4 승인: 재판정 대상은 현지 날짜가 오늘보다 이전인 일자, 드리프트는 로그·종료 코드 1로만 알림(자동 복구 없음), tick complete는 T가 n의 배수가 되는 즉시, 이전 일자의 늦은 tick은 tick 스트림에서 무시, 구독자 수 상한 없음(부하 측정 후 판단), day_index는 확정 기록·점검 용도(조회 T_d는 day_stats) | PO |
| 2026-10-02 | Sprint 5는 PM이 시나리오 설계·측정 후 보고하고, 비기능 목표치는 그 결과를 보고 PO가 정함 | PO |
| 2026-10-02 | git 운영 원칙은 PM이 팀과 상의해 정함. PM이 로컬 커밋·태그를 직접 실행(전역 지침의 이 프로젝트 한정 예외), push·되돌리기 어려운 작업 금지 | PO |
| 2026-10-02 | (개정) PO의 제안에 따라 팀원별 브랜치·worktree로 작업하고 각자 자기 브랜치에 커밋, PM이 검토 후 병합·태그. `baseline` 이후 작업부터 적용 | PO 제안, PM |
| 2026-10-02 | 원격 저장소 `origin`(github.com/yonghee-mirae/ntick)을 이 프로젝트 한정으로 자유롭게 활용(push 포함). PO가 다른 PC에서 확인할 수 있게 결과물을 충실히 동기화 | PO |
