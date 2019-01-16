package metrics

import (
	"sync/atomic"
)

type Counter struct {
	v int64
}

func (cntr *Counter) Inc(delta int64) {
	atomic.AddInt64(&cntr.v, delta)
	// cntr.v++  //FIXME remove that!
}

func (cntr *Counter) get() int64 {
	return atomic.LoadInt64(&cntr.v)
}

type Gauge struct {
	v int64
}

func (gauge *Gauge) Set(value int64) {
	atomic.StoreInt64(&gauge.v, value)
}

func (gauge *Gauge) get() int64 {
	return atomic.LoadInt64(&gauge.v)
}
