package metrics

import (
	"fmt"
	"sync"

	"github.com/awesome-flow/flow/pkg/config"
)

var (
	counters = &sync.Map{}
)

type Metric interface{}

func Initialize(sysCfg *config.CfgBlockSystem) error {

	if !sysCfg.Metrics.Enabled {
		return fmt.Errorf("Metrics module is disabled")
	}

	if t := sysCfg.Metrics.Receiver.Type; t != "graphite" {
		return fmt.Errorf("Metrics backend %s is not implemented", t)
	}

	// TODO: refactor this module and make it backend-agnostic

	err := RunGraphiteReceiver(sysCfg.Metrics.Receiver.Params, sysCfg.Metrics.Interval)
	if err != nil {
		return fmt.Errorf("Can not run metrics receiver: %v", err)
	}

	return nil
}

//Given same name returns pointer to the same Counter,
// which can be used whole program lifetime.
// FIXME using same name for different metrics types will panic.
func GetCounter(name string) *Counter {
	cntr, _ := counters.LoadOrStore(name, &Counter{})
	return cntr.(*Counter)
}

//Given same name returns pointer to the same Gauge,
// which can be used whole program lifetime.
func GetGauge(name string) *Gauge {
	gauge, _ := counters.LoadOrStore(name, &Gauge{})
	return gauge.(*Gauge)
}

func GetAllMetrics() map[string]Metric {

	res := make(map[string]Metric)

	counters.Range(func(k interface{}, val interface{}) bool {
		if metric, ok := val.(Metric); ok {
			res[k.(string)] = metric
		}
		return true
	})
	return res
}

func CounterRegistered(name string) bool {
	_, ok := counters.Load(name)
	return ok
}
