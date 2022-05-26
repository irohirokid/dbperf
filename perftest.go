package main

import (
	"fmt"
	"os"
	"time"

	"github.com/irohiroki/spanner-performance-test/db"
	"github.com/irohiroki/spanner-performance-test/result"
	"github.com/montanaflynn/stats"
)

var numLoaders = 3
var reqPerSec = 100
var testDurationSpec = "10s"

func loader(appDb db.Client, start time.Time, reqChan chan int8, statTicker <-chan time.Time, statChan chan result.Stat, termChan chan any) {
	statInterval, err := time.ParseDuration("1s")
	if err != nil {
		panic("Invalid duration")
	}

	resTimes := make(stats.Float64Data, reqPerSec*int(statInterval.Seconds()))
	numErr := 0
Loop:
	for {
		select {
		case reqCode := <-reqChan:
			if reqCode == 0 {
				break Loop
			}
			resTime, err := appDb.MeasureTransaction()
			if err != nil {
				fmt.Fprintf(os.Stderr, "measureTransaction: %v\n", err.Error())
				numErr++
			}
			resTimes = append(resTimes, float64(resTime.Microseconds())/1000)
		case <-statTicker:
			if len(resTimes) == 0 {
				break
			}

			avr, err := resTimes.Mean()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Mean: %v\n", err.Error())
			}

			med, err := resTimes.Median()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Median: %v\n", err.Error())
			}

			p95, err := stats.Percentile(resTimes, 95)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Percentile 95: %v\n", err.Error())
			}

			p99, err := stats.Percentile(resTimes, 99)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Percentile 99: %v\n", err.Error())
			}

			wst, err := resTimes.Max()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Max: %v\n", err.Error())
			}

			statChan <- result.Stat{
				Elapsed: time.Since(start),
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
	termChan <- 1
}

func perfTest(appDb db.Client) error {
	statChan := make(chan result.Stat)
	go statPrinter(statChan)

	testDuration, err := time.ParseDuration(testDurationSpec)
	if err != nil {
		return err
	}

	start := time.Now()
	reqChan := make(chan int8, reqPerSec*int(testDuration.Seconds()))
	statTicker := make(chan time.Time)
	termChan := make(chan any)
	for i := 0; i < numLoaders; i++ {
		go loader(appDb, start, reqChan, statTicker, statChan, termChan)
	}

	reqTicker := time.Tick(time.Second)
	for {
		for i := 0; i < reqPerSec; i++ {
			reqChan <- 1
		}

		t := <-reqTicker
		for i := 0; i < numLoaders; i++ {
			statTicker <- t
		}

		if time.Since(start) > testDuration {
			for i := 0; i < numLoaders; i++ {
				reqChan <- 0
			}
			for i := 0; i < numLoaders; i++ {
				<-termChan
			}
			break
		}
	}
	return nil
}

func statPrinter(statChan chan result.Stat) {
	for {
		fmt.Printf("%v\n", <-statChan)
	}
}
