// Command ntickctl runs maintenance tasks on the data directory.
//
//	ntickctl closeout -data DIR [-date YYYYMMDD] [-tz Asia/Seoul]
//	ntickctl rejudge  -data DIR -date YYYYMMDD [-symbol S] [-dry-run] [-tz Asia/Seoul]
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"ntick/internal/dayindex"
	"ntick/internal/rejudge"
	"ntick/internal/tick"
)

func main() {
	if len(os.Args) < 2 || (os.Args[1] != "closeout" && os.Args[1] != "rejudge") {
		fmt.Fprintln(os.Stderr, "usage: ntickctl closeout|rejudge [flags]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	data := fs.String("data", "./data", "data directory")
	date := fs.String("date", "", "local date YYYYMMDD (closeout default: today)")
	symbol := fs.String("symbol", "", "rejudge only this symbol (default: all)")
	dry := fs.Bool("dry-run", false, "rejudge: report only, change nothing")
	tz := fs.String("tz", "Asia/Seoul", "exchange time zone")
	fs.Parse(os.Args[2:])
	loc, err := time.LoadLocation(*tz)
	if err != nil {
		fatal(err)
	}
	now := time.Now()

	if os.Args[1] == "closeout" {
		d := *date
		if d == "" {
			d = now.In(loc).Format("20060102")
		}
		reps, err := dayindex.Closeout(*data, d)
		drift := 0
		for _, r := range reps {
			fmt.Printf("%s/%s valid_count=%d drift=%v\n", r.Date, r.Symbol, r.ValidCount, r.Drift)
			if len(r.Drift) > 0 {
				drift++
			}
		}
		if err != nil {
			fatal(err)
		}
		if drift > 0 {
			os.Exit(1)
		}
		return
	}

	sums, err := rejudge.Rejudge(*data, loc, *date, *symbol, now, *dry, tick.DefaultFilter)
	for _, s := range sums {
		fmt.Printf("%s/%s to_invalid=%d to_valid=%d valid %d -> %d\n", s.Date, s.Symbol, s.ToInvalid, s.ToValid, s.ValidBefore, s.ValidAfter)
	}
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
