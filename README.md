# ntick

체결 시세에서 유효 체결만으로 시간 캔들과 n-tick 캔들(OHLCV)을 산출해 REST와 WebSocket으로 제공하는 서비스. 저장소는 일자·종목별 SQLite 파일이다. 요구사항은 [`docs/01.PRD.md`](docs/01.PRD.md), 설계는 [`docs/architecture.md`](docs/architecture.md), 진행과 결정 기록은 [`docs/sprint.md`](docs/sprint.md)에 있다.

## 빌드와 실행

Go 1.27 이상이 필요하다 (cgo 불필요).

```
go build -o bin/ ./cmd/...

# 1) 목업 시세 발생기 (TCP 서버)
bin/mockfeed -addr 127.0.0.1:9000 -symbols 5 -days 3 -perday 2000 -rate 1000 \
  -anomaly 0.05 -reversal 0.05 -dup 0.05 -truth truth.csv

# 2) 수집 + 조회 서버
bin/ntick -feed 127.0.0.1:9000 -data ./data -http :8080
```

`ntick` 플래그: `-feed` 피드 주소, `-data` 데이터 디렉터리, `-tz` 거래소 타임존(기본 Asia/Seoul), `-http` REST·WS 주소(비우면 비활성), `-close` 장 마감 시각 HH:MM(기본 20:00, 비우면 마감 확정 비활성).

## API

```
GET /candles?symbol=S&type=tick&n=100&m=200                       # 최신 캔들부터
GET /candles?symbol=S&type=time&interval=5m&from=<ms>&to=<ms>     # 시간 오름차순, [from, to)
WS  /candles/stream?symbol=S&type=tick&n=100
WS  /candles/stream?symbol=S&type=time&interval=1m
```

가격·수량은 최소 단위 정수, 시각은 epoch ms다. 상한은 n ≤ 10,000, m ≤ 1,000, 최신 30개 일자 파일이다. 자세한 응답 형식과 오류는 `docs/architecture.md`를 본다.

## 운영 도구

```
bin/ntickctl closeout -data ./data [-date YYYYMMDD]               # 장 마감 확정(day_index)과 정합성 점검, 드리프트가 있으면 종료 코드 1
bin/ntickctl rejudge  -data ./data -date YYYYMMDD [-symbol S] [-dry-run]   # 오늘 이전 일자의 invalid 일괄 재판정
```

## 시험

```
go vet ./... && go test -count=1 ./...        # 단위·통합 시험
scripts/e2e.sh [--kill9]                       # mockfeed -> ntick -> 검증 (api, stream, ntick, time, 재판정 복구)
scripts/fault.sh all                           # 장애 주입 8개 시나리오 (약 6~7분)
```

`bin/tclient`의 검증 명령은 정답 파일(`truth.csv`)에서 독립적으로 기대값을 계산해 저장 결과와 비교한다: `check-integrity`, `verify`, `verify-ntick`, `verify-time`, `verify-api`, `verify-stream`, `verify-subsequence`, `count-ticks`, `stress-read`.

## 저장소 규칙

작업 방식(브랜치, 커밋, 게이트, 원격 동기화)은 `docs/sprint.md`의 "Git 운영 원칙"을 따른다.
