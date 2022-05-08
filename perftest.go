package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/montanaflynn/stats"
)

var numLoaders = 3
var reqPerSec = 100
var testDurationSpec = "10s"

type Stat struct {
	elapsed time.Duration
	err     int
	avr     float64
	med     float64
	p95     float64
	p99     float64
	wst     float64
	remain  int
}

func (s Stat) String() string {
	return fmt.Sprintf("%s\t%d\t%.02f\t%.02f\t%.02f\t%.02f\t%.02f\t%d", s.elapsed.Round(time.Second).String(), s.err, s.avr, s.med, s.p95, s.p99, s.wst, s.remain)
}

func (appSpanner AppSpanner) perfTest() error {
	statChan := make(chan Stat)
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
		go loader(appSpanner, start, reqChan, statTicker, statChan, termChan)
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

func (appSpanner AppSpanner) measureTransaction() (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	start := time.Now()
	_, err := appSpanner.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		userRow, err := txn.ReadRow(ctx, "Users", spanner.Key{1}, []string{"Gold"}) // TODO disparse id
		if err != nil {
			return err
		}

		var gold int64
		err = userRow.Column(0, &gold)
		if err != nil {
			return err
		}

		if gold < 5 {
			return errors.New("not enough gold")
		}

		var userItemRow *spanner.Row
		userItemRow, err = txn.ReadRow(ctx, "UserItems", spanner.Key{1}, []string{"Amount"})
		if err != nil {
			return err
		}

		var amount int64
		err = userItemRow.Column(0, &amount)
		if err != nil {
			return err
		}

		txn.BufferWrite([]*spanner.Mutation{
			spanner.Update("Users", []string{"Id", "Gold"}, []interface{}{1, gold - 5}),
			spanner.Update("UserItems", []string{"Id", "Amount"}, []interface{}{1, amount + 1}),
		})
		return nil
	})
	return time.Since(start), err
}

func loader(appSpanner AppSpanner, start time.Time, reqChan chan int8, statTicker <-chan time.Time, statChan chan Stat, termChan chan any) {
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
			resTime, err := appSpanner.measureTransaction()
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

			statChan <- Stat{
				elapsed: time.Since(start),
				err:     numErr,
				avr:     avr,
				med:     med,
				p95:     p95,
				p99:     p99,
				wst:     wst,
				remain:  len(reqChan),
			}

			resTimes = resTimes[:0]
			numErr = 0
		}
	}
	termChan <- 1
}

func statPrinter(statChan chan Stat) {
	for {
		fmt.Printf("%v\n", <-statChan)
	}
}
