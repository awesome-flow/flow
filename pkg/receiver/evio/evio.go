package receiver

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/tidwall/evio"
	"github.com/whiteboxio/flow/pkg/core"
	"github.com/whiteboxio/flow/pkg/metrics"
)

type Evio struct {
	Name   string
	events *evio.Events
	*core.Connector
}

func NewEvio(name string, params core.Params) (core.Link, error) {
	events := &evio.Events{}

	if numLoops, ok := params["num_loops"]; ok {
		events.NumLoops = numLoops.(int)
	} else {
		events.NumLoops = -1 // sets to GOMAXPROCS
	}

	listIntf, ok := params["listeners"]
	if !ok {
		return nil, fmt.Errorf("Failed to initialize evio: missing listeners")
	}

	ev := &Evio{
		name,
		events,
		core.NewConnector(),
	}

	events.Data = func(ec evio.Conn, buf []byte) (out []byte, action evio.Action) {
		metrics.GetCounter("receiver.evio.received").Inc(1)
		if err := ev.Send(core.NewMessage(nil, buf)); err != nil {
			log.Errorf("Failed to send evio message: %s", err)
			metrics.GetCounter("receiver.evio.failed").Inc(1)
			return
		}
		metrics.GetCounter("receiver.evio.sent").Inc(1)
		return
	}

	listeners := make([]string, len(listIntf.([]interface{})))
	for ix, li := range listIntf.([]interface{}) {
		listeners[ix] = li.(string)
	}
	go func() {
		if err := evio.Serve(*events, listeners...); err != nil {
			log.Fatalf("Failed to start evio: %s", err)
		}
	}()

	return ev, nil
}
