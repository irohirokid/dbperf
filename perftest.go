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
	"golang.org/x/time/rate"
)

type statData struct {
	start    time.Time
	reqChan  chan struct{}
	resTimes stats.Float64Data
	numErr   int
}

func (sd *statData) report() (*result.Stat, error) {
	avr, err := sd.resTimes.Mean()
	if err != nil {
		return nil, fmt.Errorf("mean: %s", err.Error())
	}

	med, err := sd.resTimes.Median()
	if err != nil {
		return nil, fmt.Errorf("median: %s", err.Error())
	}

	p95, err := stats.Percentile(sd.resTimes, 95)
	if err != nil {
		return nil, fmt.Errorf("percentile 95: %s", err.Error())
	}

	p99, err := stats.Percentile(sd.resTimes, 99)
	if err != nil {
		return nil, fmt.Errorf("percentile 99: %s", err.Error())
	}

	wst, err := sd.resTimes.Max()
	if err != nil {
		return nil, fmt.Errorf("max: %s", err.Error())
	}

	stat := result.Stat{
		Elapsed: time.Since(sd.start),
		Count:   len(sd.resTimes),
		Err:     sd.numErr,
		Avr:     avr,
		Med:     med,
		P95:     p95,
		P99:     p99,
		Wst:     wst,
		Remain:  len(sd.reqChan),
	}

	sd.resTimes = sd.resTimes[:0]
	sd.numErr = 0

	return &stat, nil
}

func (sd *statData) appendResTime(t time.Duration) {
	sd.resTimes = append(sd.resTimes, float64(t.Microseconds())/1000)
}

func (sd *statData) empty() bool {
	return len(sd.resTimes) == 0
}

func (sd *statData) incErr() {
	sd.numErr++
}

func loader(appDb db.Client, reqChan chan struct{}, statChan chan result.Stat) error {
	var err error
	var stat *result.Stat
	sd := &statData{
		start:    time.Now(),
		reqChan:  reqChan,
		resTimes: make(stats.Float64Data, 0, *reqPerSec**interval),
		numErr:   0,
	}
	statTicker := time.Tick(time.Second)
Loop:
	for {
		select {
		case _, ok := <-reqChan:
			if !ok {
				if !sd.empty() {
					stat, err = sd.report()
					if err != nil {
						return err
					}
					statChan <- *stat
				}
				break Loop
			}
			_start := time.Now()
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
			resTime := time.Since(_start)
			if err != nil {
				fmt.Fprintf(os.Stderr, "On operation: %v\n", err.Error())
				sd.incErr()
			}
			sd.appendResTime(resTime)
		case <-statTicker:
			if len(reqChan) > *reqPerSec {
				return errors.New("*** Request overflow ***")
			}

			if sd.empty() {
				break
			}

			stat, err = sd.report()
			if err != nil {
				return err
			}
			statChan <- *stat
		}
	}
	return nil
}

func parentLoader(ctx context.Context, reqChan chan struct{}) {
	limitter := rate.NewLimiter(rate.Limit(*reqPerSec), 1)
	for i := 0; i < *reqPerSec**duration; i++ {
		limitter.Wait(ctx)
		reqChan <- struct{}{}
	}
	time.AfterFunc(time.Second, func() {
		close(reqChan)
	})
}

func perfTest(appDb db.Client) error {
	statChan := make(chan result.Stat)
	go statPrinter(statChan)

	reqChan := make(chan struct{}, *reqPerSec**duration)
	eg, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < *numLoaders; i++ {
		eg.Go(func() error {
			return loader(appDb, reqChan, statChan)
		})
	}

	go parentLoader(ctx, reqChan)

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
