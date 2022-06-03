package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/irohirokid/dbperf/db"
	"github.com/irohirokid/dbperf/result"
	"github.com/montanaflynn/stats"
	"golang.org/x/sync/errgroup"
)

func loader(appDb db.Client, start time.Time, reqChan chan struct{}, statTicker <-chan time.Time, statChan chan result.Stat) error {
	resTimes := make(stats.Float64Data, 0, *reqPerSec**interval)
	numErr := 0
Loop:
	for {
		select {
		case _, ok := <-reqChan:
			if !ok {
				break Loop
			}
			var err error
			start := time.Now()
			switch *operation {
			case "c":
				break // TODO
			case "r":
				err = appDb.SimpleRead()
			case "u":
				break // TODO
			case "cr":
				err = appDb.ConsistentRead()
			case "tw":
				err = appDb.TransactWrite()
			}
			resTime := time.Since(start)
			if err != nil {
				fmt.Fprintf(os.Stderr, "On operation: %v\n", err.Error())
				numErr++
			}
			resTimes = append(resTimes, float64(resTime.Microseconds())/1000)
		case <-statTicker:
			if len(reqChan) > *reqPerSec {
				return errors.New("*** Request overflow ***")
			}

			if len(resTimes) == 0 {
				break
			}

			avr, err := resTimes.Mean()
			if err != nil {
				return fmt.Errorf("mean: %s", err.Error())
			}

			med, err := resTimes.Median()
			if err != nil {
				return fmt.Errorf("median: %s", err.Error())
			}

			p95, err := stats.Percentile(resTimes, 95)
			if err != nil {
				return fmt.Errorf("percentile 95: %s", err.Error())
			}

			p99, err := stats.Percentile(resTimes, 99)
			if err != nil {
				return fmt.Errorf("percentile 99: %s", err.Error())
			}

			wst, err := resTimes.Max()
			if err != nil {
				return fmt.Errorf("max: %s", err.Error())
			}

			statChan <- result.Stat{
				Elapsed: time.Since(start),
				Count:   len(resTimes),
				Err:     numErr,
				Avr:     avr,
				Med:     med,
				P95:     p95,
				P99:     p99,
				Wst:     wst,
				Remain:  len(reqChan),
			}

			resTimes = resTimes[:0]
			numErr = 0
		}
	}
	return nil
}

func parentLoader(start time.Time, reqChan chan struct{}, statTicker chan time.Time) {
	testDuration := time.Duration(*duration * int(time.Second))
	reqTicker := time.Tick(time.Second)
	queueRequests(reqChan)
Loop:
	for {
		select {
		case t := <-reqTicker:
			if int(time.Since(start).Seconds())%*interval == 0 {
				for i := 0; i < *numLoaders; i++ {
					statTicker <- t
				}
			}

			if time.Since(start) > testDuration {
				close(reqChan)
				break Loop
			} else {
				queueRequests(reqChan)
			}
		}
	}
}

func perfTest(appDb db.Client) error {
	statChan := make(chan result.Stat)
	go statPrinter(statChan)

	start := time.Now()
	reqChan := make(chan struct{}, *reqPerSec**duration)
	statTicker := make(chan time.Time)
	eg, _ := errgroup.WithContext(context.Background())
	for i := 0; i < *numLoaders; i++ {
		eg.Go(func() error {
			return loader(appDb, start, reqChan, statTicker, statChan)
		})
	}

	go parentLoader(start, reqChan, statTicker)

	err := eg.Wait()
	return err
}

func queueRequests(reqChan chan struct{}) {
	for i := 0; i < *reqPerSec; i++ {
		reqChan <- struct{}{}
	}
}

func statPrinter(statChan chan result.Stat) {
	for {
		fmt.Printf("%v\n", <-statChan)
	}
}
