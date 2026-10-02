package main

// Perf helpers for Sprint 5 measurements. They are dispatched from init() so main.go stays untouched.
//
//	tclient bench-feed -addr A -symbols N -rate R -duration D [-hot SYM] [-churn]   (feed server, ts=now, price=send time in us, qty=1)
//	tclient bench-http -url URL -clients N -duration D                               (REST load, latency percentiles, status counts)
//	tclient bench-sink -addr A                                                       (feed client that discards: generator-side limit)
//	tclient bench-ws -base ws://H:P -symbols a,b -subs N -duration D [-slow K]      (WS subscribers, send->receive latency)

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"ntick/internal/wire"
)

func init() {
	if len(os.Args) < 2 || !strings.HasPrefix(os.Args[1], "bench-") {
		return
	}
	args := os.Args[2:]
	switch os.Args[1] {
	case "bench-feed":
		benchFeed(args)
	case "bench-http":
		benchHTTP(args)
	case "bench-ws":
		benchWS(args)
	case "bench-sink":
		benchSink(args)
	default:
		fmt.Fprintln(os.Stderr, "unknown bench subcommand")
		os.Exit(2)
	}
	os.Exit(0)
}

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	return d[min(len(d)-1, int(float64(len(d))*p))]
}

func summarize(name string, d []time.Duration) {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	fmt.Printf("%s n=%d p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms\n", name, len(d),
		ms(pct(d, .50)), ms(pct(d, .95)), ms(pct(d, .99)), ms(pct(d, 1)))
}

func ms(d time.Duration) float64 { return float64(d) / 1e6 }

// benchFeed serves one feed connection: rate msgs/s (0 = unthrottled) for duration over nsym symbols.
// ts = now (ms), price = now in microseconds (the WS client derives latency from it), qty = 1.
// -churn gives every tick its own day (ts advances one day per tick of a symbol) to force a new day file per tick.
func benchFeed(args []string) {
	fs := flag.NewFlagSet("bench-feed", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:9000", "listen address")
	nsym := fs.Int("symbols", 1, "number of symbols (SB000000...)")
	rate := fs.Int("rate", 1000, "messages per second (0 = unthrottled)")
	dur := fs.Duration("duration", 10*time.Second, "how long to send")
	churn := fs.Bool("churn", false, "one new day file per tick")
	fs.Parse(args)
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	conn, err := ln.Accept()
	if err != nil {
		os.Exit(1)
	}
	w := bufio.NewWriterSize(conn, 256<<10)
	start := time.Now()
	var sent int64
	day0 := time.Date(2030, 1, 1, 9, 0, 0, 0, time.UTC).UnixMilli()
	for {
		el := time.Since(start)
		if el >= *dur {
			break
		}
		target := sent + 1000 // unthrottled: chunks of 1000
		if *rate > 0 {
			target = int64(float64(el) / float64(time.Second) * float64(*rate))
		}
		for ; sent < target; sent++ {
			now := time.Now()
			ts, price := now.UnixMilli(), now.UnixMicro()
			if *churn {
				ts = day0 + (sent/int64(*nsym))*86400000
			}
			b, _ := wire.Encode(wire.Message{Symbol: "SB" + strconv.Itoa(100000+int(sent)%*nsym), Ts: ts, Price: price, Qty: 1})
			w.Write(b)
		}
		if err := w.Flush(); err != nil {
			break
		}
		if *rate > 0 {
			time.Sleep(time.Millisecond)
		}
	}
	w.Flush()
	conn.Close()
	el := time.Since(start)
	fmt.Printf("feed sent=%d secs=%.2f rate=%.0f/s\n", sent, el.Seconds(), float64(sent)/el.Seconds())
}

func benchHTTP(args []string) {
	fs := flag.NewFlagSet("bench-http", flag.ExitOnError)
	url := fs.String("url", "", "request URL")
	clients := fs.Int("clients", 1, "concurrent clients")
	dur := fs.Duration("duration", 10*time.Second, "how long")
	count := fs.Int("count", 0, "total requests instead of duration (0 = use duration)")
	warm := fs.Int("warm", 2, "warm-up requests, not counted")
	fs.Parse(args)
	cl := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *clients + 1}}
	do := func() (time.Duration, int, int64) {
		t := time.Now()
		resp, err := cl.Get(*url)
		if err != nil {
			return time.Since(t), -1, 0
		}
		n, _ := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return time.Since(t), resp.StatusCode, n
	}
	for i := 0; i < *warm; i++ {
		do()
	}
	var mu sync.Mutex
	var lat []time.Duration
	codes := map[int]int{}
	var bytes, issued atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if *count > 0 {
					if issued.Add(1) > int64(*count) {
						return
					}
				} else if time.Since(start) >= *dur {
					return
				}
				d, code, n := do()
				bytes.Add(n)
				mu.Lock()
				lat = append(lat, d)
				codes[code]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	el := time.Since(start)
	fmt.Printf("http clients=%d reqs=%d secs=%.2f rps=%.1f avg_bytes=%d codes=%v\n", *clients, len(lat), el.Seconds(),
		float64(len(lat))/el.Seconds(), int(bytes.Load())/max(len(lat), 1), codes)
	summarize("http latency", lat)
}

// benchWS opens subs subscribers spread round-robin over the symbols (tick candles, n=1000) and measures
// receive time minus the send time encoded in the candle close (bench-feed price, microseconds).
// The first slow subscribers connect but never read, to provoke the server-side buffer overflow.
func benchWS(args []string) {
	fs := flag.NewFlagSet("bench-ws", flag.ExitOnError)
	base := fs.String("base", "ws://127.0.0.1:8080", "server base URL")
	syms := fs.String("symbols", "SB100000", "comma-separated symbols")
	subs := fs.Int("subs", 1, "subscribers")
	slow := fs.Int("slow", 0, "of which never read")
	dur := fs.Duration("duration", 10*time.Second, "how long to listen")
	fs.Parse(args)
	list := strings.Split(*syms, ",")
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	var mu sync.Mutex
	var lat, latC []time.Duration // update messages / complete messages
	var msgs, dropped, otherClose, connFail, slowDropped atomic.Int64
	reasons := map[string]int{}
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	for i := 0; i < *subs; i++ {
		wg.Add(1)
		ready.Add(1)
		go func(i int) {
			defer wg.Done()
			url := fmt.Sprintf("%s/candles/stream?symbol=%s&type=tick&n=1000", *base, list[i%len(list)])
			c, _, err := websocket.Dial(ctx, url, nil)
			ready.Done()
			if err != nil {
				connFail.Add(1)
				return
			}
			defer c.CloseNow()
			if i < *slow {
				// never reads while the run lasts; afterwards drain the backlog and see whether the server dropped it (1013)
				<-ctx.Done()
				rctx, rcancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer rcancel()
				nread := 0
				for {
					if _, _, err := c.Read(rctx); err != nil {
						if websocket.CloseStatus(err) == websocket.StatusTryAgainLater {
							slowDropped.Add(1)
						}
						mu.Lock()
						reasons[fmt.Sprintf("slow-reader end after %d msgs: %.60s", nread/50*50, err.Error())]++
						mu.Unlock()
						return
					}
					nread++
				}
			}
			c.SetReadLimit(1 << 20)
			var local, localC []time.Duration
			for {
				_, b, err := c.Read(ctx)
				if err != nil {
					if ctx.Err() == nil {
						if websocket.CloseStatus(err) == websocket.StatusTryAgainLater {
							dropped.Add(1)
						} else {
							otherClose.Add(1)
							mu.Lock()
							reasons[err.Error()]++
							mu.Unlock()
						}
					}
					break
				}
				recv := time.Now().UnixMicro()
				msgs.Add(1)
				var m struct {
					Type   string
					Candle *struct{ Close int64 }
				}
				if json.Unmarshal(b, &m) != nil || m.Candle == nil || m.Type == "snapshot" {
					continue
				}
				if d := recv - m.Candle.Close; d >= 0 && d < 60e6 {
					if m.Type == "complete" {
						localC = append(localC, time.Duration(d)*time.Microsecond)
					} else {
						local = append(local, time.Duration(d)*time.Microsecond)
					}
				}
			}
			mu.Lock()
			lat = append(lat, local...)
			latC = append(latC, localC...)
			mu.Unlock()
		}(i)
	}
	ready.Wait()
	fmt.Printf("ws connected (%d failed) at %s\n", connFail.Load(), time.Now().Format("15:04:05.000"))
	wg.Wait()
	fmt.Printf("ws subs=%d slow=%d slow_dropped_1013=%d msgs=%d dropped_1013=%d other_close=%d reasons=%v\n", *subs, *slow, slowDropped.Load(), msgs.Load(), dropped.Load(), otherClose.Load(), reasons)
	summarize("ws update send->recv latency (age of newest tick)", lat)
	summarize("ws complete send->recv latency (age of the tick that closed the candle)", latC)
}

// benchSink connects to a feed server (e.g. mockfeed -rate 0), reads and discards until EOF, and reports msgs/s.
func benchSink(args []string) {
	fs := flag.NewFlagSet("bench-sink", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:9000", "feed address")
	bulk := fs.Bool("bulk", false, "read in big chunks with short pauses (measures the generator, not the reader)")
	fs.Parse(args)
	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	start := time.Now()
	n := 0
	if *bulk { // big reads with a short pause so the sender is not paced by wake-ups of a fast reader
		buf := make([]byte, 4<<20)
		total := 0
		for {
			k, err := conn.Read(buf)
			total += k
			if err != nil {
				break
			}
			time.Sleep(200 * time.Microsecond)
		}
		n = total / wire.Size
	} else {
		r := wire.NewReader(bufio.NewReaderSize(conn, 64<<10))
		for {
			if _, err := r.Next(); err != nil {
				break
			}
			n++
		}
	}
	el := time.Since(start)
	fmt.Printf("sink msgs=%d secs=%.2f rate=%.0f/s\n", n, el.Seconds(), float64(n)/el.Seconds())
}
