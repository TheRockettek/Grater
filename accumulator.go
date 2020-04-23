package main

import (
	"sync"
	"time"
)

const (
	// DefaultTotalSamples is the limit of total samples allowed for the accumulator values
	DefaultTotalSamples = 3600
)

// Accumulator allows for easy concurrent number accumulation
type Accumulator struct {
	sync.Mutex
	values []int64 // A store of all the values to date
	acc    int64   // The current value
	incr   int64   // Total increments ever
}

// Increment the current second's counter.
func (ac *Accumulator) Increment() {
	ac.Lock()
	defer ac.Unlock()
	ac.acc++
	ac.incr++
}

// GetLastSecond gets the counter from the last second.
func (ac *Accumulator) GetLastSecond() int64 {
	ac.Lock()
	defer ac.Unlock()

	if len(ac.values) > 0 {
		return ac.values[len(ac.values)-1]
	}
	return 0
}

// GetLastMinute gets the total count of increment calls in the last minute.
func (ac *Accumulator) GetLastMinute() int64 {
	ac.Lock()
	defer ac.Unlock()

	// Get (up to) the position of the last 60 seconds worth of data.
	sliceStart := len(ac.values) - 60
	if sliceStart < 0 {
		sliceStart = 0
	}
	slice := ac.values[sliceStart:len(ac.values)]

	// The final value that we want to accumulate to.
	var acc int64

	for _, val := range slice {
		acc += val
	}
	return acc
}

// GetLastHour gets the total count of increment calls in the last hour.
func (ac *Accumulator) GetLastHour() int64 {
	ac.Lock()
	defer ac.Unlock()

	// The final value that we want to accumulate to.
	var acc int64
	for _, val := range ac.values {
		acc += val
	}

	return acc
}

// Actually starts the accumulatorImpl, aggregating data in to the internal data
// structures that lets us track what each count is.
func (ac *Accumulator) run() {
	ticks := time.Tick(1 * time.Second)

	for range ticks {
		ac.Lock()
		ac.values = append(ac.values, ac.acc)
		if len(ac.values) > DefaultTotalSamples {
			ac.values = ac.values[1:DefaultTotalSamples]
		}
		ac.acc = 0
		ac.Unlock()
	}
}

// NewAccumulator instantiates and starts a new accumulator.
func NewAccumulator() *Accumulator {
	acc := &Accumulator{}
	go acc.run()
	return acc
}
