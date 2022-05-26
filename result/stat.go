package result

import (
	"fmt"
	"time"
)

type Stat struct {
	Elapsed time.Duration
	Err     int
	Avr     float64
	Med     float64
	P95     float64
	P99     float64
	Wst     float64
	Remain  int
}

func (s Stat) String() string {
	return fmt.Sprintf("%s\t%d\t%.02f\t%.02f\t%.02f\t%.02f\t%.02f\t%d", s.Elapsed.Round(time.Second).String(), s.Err, s.Avr, s.Med, s.P95, s.P99, s.Wst, s.Remain)
}
