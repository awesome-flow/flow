package metrics

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	graphite "github.com/marpaia/graphite-golang"
	log "github.com/sirupsen/logrus"
	"github.com/awesome-flow/flow/pkg/config"
)

type Counter struct {
	v int64
}

func (cntr *Counter) Inc(delta int64) {
	atomic.AddInt64(&cntr.v, delta)
}

var (
	namespace    = ""
	counters     = &sync.Map{}
	sendInterval = 1 * time.Second

	grph *graphite.Graphite = nil
)

func Initialize(sysCfg *config.CfgBlockSystem) error {

	if !sysCfg.Metrics.Enabled {
		return fmt.Errorf("Metrics module is disabled")
	}

	if t := sysCfg.Metrics.Receiver.Type; t != "graphite" {
		return fmt.Errorf("Metrics backend %s is not implemented", t)
	}

	// TODO: refactor this module and make it backend-agnostic
	// TODO: Initialization should happen exactly once

	grphHost, _ := sysCfg.Metrics.Receiver.Params["host"]
	grphPortStr, _ := sysCfg.Metrics.Receiver.Params["port"]
	grphPort, err := strconv.Atoi(grphPortStr)
	if err != nil {
		return err
	}

	namespace = sysCfg.Metrics.Receiver.Params["namespace"]
	sendInterval = time.Duration(sysCfg.Metrics.Interval) * time.Second

	grph, err = graphite.NewGraphite(grphHost, grphPort)
	if err != nil {
		return err
	}

	chIn := make(chan bool, 1)

	go func() {
		for {
			<-chIn
			if err := sendMetrics(); err != nil {
				log.Warnf("Metrics module failed to send metrics: %s", err)
			}
			chIn <- true
		}
	}()
	chIn <- true

	return nil
}

func sendMetrics() error {
	time.Sleep(sendInterval)
	metrics := make([]graphite.Metric, 0)
	now := time.Now().Unix()
	counters.Range(func(key interface{}, value interface{}) bool {
		metrics = append(metrics, graphite.NewMetric(
			namespace+"."+key.(string),
			strconv.FormatInt(value.(*Counter).v, 10),
			now))
		return true
	})
	if len(metrics) > 0 {
		log.Debug("Sending graphite metrics now")
		if err := grph.SendMetrics(metrics); err != nil {
			return err
		}
	}
	return nil
}

func GetCounter(name string) *Counter {
	cntr, _ := counters.LoadOrStore(name, &Counter{})
	return cntr.(*Counter)
}

func GetAll() map[string]int64 {
	res := make(map[string]int64)
	counters.Range(func(k interface{}, val interface{}) bool {
		res[k.(string)] = val.(*Counter).v
		return true
	})
	return res
}

func CounterRegistered(name string) bool {
	_, ok := counters.Load(name)
	return ok
}
