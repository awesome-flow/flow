package metrics

import (
	"sync"
	"sync/atomic"
)

type Counter struct {
	v int64
}

func (cntr *Counter) Inc(delta int64) {
	atomic.AddInt64(&cntr.v, delta)
}

var (
	namespace = ""
	counters  = &sync.Map{}
)

func Initialize(nmspc string) error {
	namespace = nmspc
	return nil
}

func GetCounter(name string) *Counter {
	cntr, _ := counters.LoadOrStore(name, &Counter{})
	return cntr.(*Counter)
}

func CounterRegistered(name string) bool {
	_, ok := counters.Load(name)
	return ok
}
