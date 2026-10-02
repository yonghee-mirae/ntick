// Command ntick ingests the tick feed into per-day, per-symbol SQLite files.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"ntick/internal/api"
	"ntick/internal/dayindex"
	"ntick/internal/ingest"
	"ntick/internal/stream"
)

func main() {
	feed := flag.String("feed", "127.0.0.1:9000", "feed TCP address")
	data := flag.String("data", "./data", "data directory")
	tz := flag.String("tz", "Asia/Seoul", "exchange time zone")
	closeAt := flag.String("close", "20:00", "market close HH:MM in -tz for the day_index closeout (empty = disabled)")
	httpAddr := flag.String("http", "", "REST listen address, e.g. :8080 (empty = disabled)")
	flag.Parse()

	loc, err := time.LoadLocation(*tz)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var srv *http.Server
	var broker *stream.Broker
	if *httpAddr != "" {
		broker = stream.NewBroker()
		mux := http.NewServeMux()
		mux.Handle("/candles/stream", stream.Handler(*data, broker, loc))
		mux.Handle("/", api.Handler(*data, loc))
		// No WriteTimeout: WebSocket connections are long-lived.
		srv = &http.Server{Addr: *httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				log.Printf("http: %v", err)
				stop() // shut ingest down too
			}
		}()
	}

	if reps, err := dayindex.CatchUp(*data, loc, time.Now()); err != nil {
		log.Printf("catch-up: %v", err)
	} else if len(reps) > 0 {
		dayindex.LogReports(reps)
	}
	closeDone := make(chan struct{})
	if *closeAt != "" {
		if _, err := dayindex.NextClose(time.Now(), loc, *closeAt); err != nil {
			log.Fatal(err)
		}
		go func() {
			defer close(closeDone)
			dayindex.Schedule(*data, loc, *closeAt, ctx.Done())
		}()
	} else {
		close(closeDone)
	}

	in := ingest.New(*data, loc)
	if broker != nil {
		in.OnCommit(broker.Publish)
	}
	in.Serve(ctx, *feed)
	if srv != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		srv.Shutdown(sctx)
		cancel()
	}
	<-closeDone // ctx is cancelled once Serve returns on a signal; wait for a running closeout
	in.Close()  // flush pending batches
	if broker != nil {
		broker.Close() // sends close frames to WebSocket clients (Shutdown does not track hijacked connections)
	}
	log.Print("shutdown complete")
}
