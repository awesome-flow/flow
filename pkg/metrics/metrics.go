package metrics

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	graphite "github.com/marpaia/graphite-golang"
	log "github.com/sirupsen/logrus"
	"github.com/whiteboxio/flow/pkg/config"
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
)

func Initialize(nmspc string) error {
	namespace = nmspc

	grphHost, _ := config.GetOrDefault("metrics.graphite.host", "localhost")
	grphPortStr, _ := config.GetOrDefault("metrics.graphite.port", "2003")
	grphPort, err := strconv.Atoi(grphPortStr.(string))
	if err != nil {
		return err
	}

	graphite, err := graphite.NewGraphite(grphHost.(string), grphPort)
	if err != nil {
		return err
	}

	go sendMetrics(graphite)
	return nil
}

func sendMetrics(grph *graphite.Graphite) {
	time.Sleep(sendInterval)
	metrics := make([]graphite.Metric, 0)
	now := time.Now().Unix()
	counters.Range(func(key interface{}, value interface{}) bool {
		metrics = append(metrics, graphite.NewMetric(
			key.(string),
			strconv.FormatInt(value.(*Counter).v, 10),
			now))
		return true
	})
	if len(metrics) > 0 {
		log.Debug("Sending graphite metrics now")
		if err := grph.SendMetrics(metrics); err != nil {
			log.Warnf("Metrics module failed to send metrics: %s", err)
		}
	}
	sendMetrics(grph)
}

func GetCounter(name string) *Counter {
	cntr, _ := counters.LoadOrStore(name, &Counter{})
	return cntr.(*Counter)
}

func CounterRegistered(name string) bool {
	_, ok := counters.Load(name)
	return ok
}
