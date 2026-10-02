# Mock Feed Spec

**MOCK ASSUMPTION**: the real exchange message format is unknown. This layout is invented for development
and lives behind an adapter boundary (`internal/wire`). Replace it when the real spec arrives.

## Message layout (big-endian, fixed length = 38 bytes)

| Offset | Size | Field | Notes |
| --- | --- | --- | --- |
| 0 | 1 | type | `1` = trade (the only type; no judgment/invalid channel) |
| 1 | 1 | version | `1` |
| 2 | 12 | symbol | printable ASCII, right-padded with spaces |
| 14 | 8 | ts | int64, epoch ms (exchange trade time) |
| 22 | 8 | price | int64, smallest price unit |
| 30 | 8 | qty | int64 |

No trade number exists. The decoder checks length, type, version only; it does not judge price/qty validity.

## Framing

TCP stream (PM proposal, pending PO confirmation). Messages are concatenated with no delimiter or length
prefix; the receiver reads exactly 38 bytes per message (partial reads must be accumulated). A stream ending
mid-message is an error (`io.ErrUnexpectedEOF`).

## Generator (`cmd/mockfeed`)

Deterministic per `-seed`. One global sequence: a reconnecting client continues where the last stopped.
No resend/replay is implemented (undecided by PO). Intended for one client at a time.
Ground truth (`-truth`): CSV `idx,symbol,ts,price,qty,label`, one line per message written to a client.

Scenarios (all off by default; probabilities per tick):

| Flag | Scenario | Label |
| --- | --- | --- |
| (always) | normal tick | `ok` |
| `-anomaly` | price<=0 or qty<=0 (0 or -1) | `price_le0`, `qty_le0` |
| `-reversal` | ts earlier than the previous tick of the same symbol | `ts_reversal` |
| `-dup` | exact copy (same ms, price, qty) sent right after the original | `dup` (the copy) |
| `-disconnect N` | server closes each connection after N messages | - |

Multi-day: `-days D -perday N -tz Asia/Seoul`. Day k starts at `-start-time` (HH:MM, default 09:00) local on `-start-date` (YYYY-MM-DD, default 2026-01-05, any date incl. today) + k days, so ts crosses local midnight between days.
Rate: `-rate` msgs/sec (0 = unthrottled). Symbols: `-symbols` (names `S100000`...).
