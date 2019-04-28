package metrics

import (
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/awesome-flow/flow/pkg/cast"
	graphite "github.com/marpaia/graphite-golang"
)

func RunGraphiteReceiver(params map[string]cast.Value, interval int) error {

	grphHost := params["host"].(string)
	grphPortStr := params["port"].(string)
	grphPort, err := strconv.Atoi(grphPortStr)
	if err != nil {
		return err
	}

	namespace := params["namespace"].(string)
	sendInterval := time.Duration(interval) * time.Second

	grph, err := graphite.NewGraphite(grphHost, grphPort)
	if err != nil {
		return err
	}

	started := make(chan struct{}, 1)

	go func() {
		for {
			close(started) //FIXME remove
			time.Sleep(sendInterval)
			if err := sendMetrics(grph, namespace); err != nil {
				log.Warnf("Metrics module failed to send metrics: %s", err)
			}
		}
	}()

	<-started
	return nil
}

func sendMetrics(grph *graphite.Graphite, namespace string) error {
	metrics := make([]graphite.Metric, 0)
	now := time.Now().Unix()

	for key, metric := range GetAllMetrics() {
		switch metric := metric.(type) {

		case *Counter:
			metrics = append(metrics, graphite.NewMetric(
				namespace+"."+key,
				strconv.FormatInt(metric.Get(), 10),
				now))
		case *Gauge: //Same as counter
			metrics = append(metrics, graphite.NewMetric(
				namespace+"."+key,
				strconv.FormatInt(metric.Get(), 10),
				now))
		default:
		}
	}

	if len(metrics) > 0 {
		log.Debug("Sending graphite metrics now")
		if err := grph.SendMetrics(metrics); err != nil {
			return err
		}
	}
	return nil
}
