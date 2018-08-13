package receiver

import (
	"github.com/tidwall/evio"
	"github.com/whiteboxio/flow/pkg/core"
)

type Evio struct {
	Name   string
	events *evio.Events
	*core.Connector
}

func NewEvio(name string, params core.Params) (core.Link, error) {
	return nil, nil
}

// func NewEvioUDP(name string, params core.Params) (core.Link, error) {
// 	udp := &UDP{name, nil, core.NewConnector()}

// 	udpAddr, ok := params["bind_addr"]
// 	if !ok {
// 		return nil, fmt.Errorf("UDP receiver parameters are missing bind_addr")
// 	}

// 	log.Infof("Starting a new evio handler")

// 	var events evio.Events
// 	events.NumLoops = 4
// 	events.Data = func(c evio.Conn, in []byte) (out []byte, action evio.Action) {
// 		msg := core.NewMessage(nil, in)
// 		if sendErr := udp.Send(msg); sendErr != nil {
// 			log.Errorf("UDP failed to accept message: %s", sendErr.Error())
// 		} else {
// 			metrics.GetCounter("receiver.udp.sent").Inc(1)
// 		}
// 		return
// 	}
// 	go func() {
// 		if err := evio.Serve(events, "udp://"+udpAddr.(string)); err != nil {
// 			log.Errorf("Unable to connect evio listener: %s", err)
// 		}
// 	}()

// 	return udp, nil
// }
