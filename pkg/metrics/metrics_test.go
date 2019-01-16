package metrics

import (
	"fmt"
	"sync"
	"testing"
)

func TestCountersConcurrency(t *testing.T) {

	wg := sync.WaitGroup{}
	routines := 2
	iterations := 100
	iterations_per_counter := 10

	for routine := 0; routine < routines; routine++ {
		wg.Add(1)
		go func() {
			for n := 0; n < iterations; n++ {
				for i := 0; i < iterations_per_counter; i++ {
					c := GetCounter(fmt.Sprintf("counter_%d", n))
					c.Inc(1)
					_ = c.Get()
					GetAllMetrics()

					g := GetGauge(fmt.Sprintf("counter_%d_gauge", n))
					g.Set(int64(i))
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()

	//Check them all

	for n := 0; n < iterations; n++ {
		c := GetCounter(fmt.Sprintf("counter_%d", n))
		value := c.Get()
		if value != int64(routines*iterations_per_counter) {
			t.Errorf("Number of values does not equal %d != %d", value, routines)
			return
		}
	}
	for _, metric := range GetAllMetrics() {
		if counter, ok := metric.(*Counter); ok {
			value := counter.Get()
			if value != int64(routines*iterations_per_counter) {
				t.Errorf("Number of values does not equal %d != %d", value, routines)
				return
			}
		}
	}

}
