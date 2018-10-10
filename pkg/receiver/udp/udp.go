package receiver

import (
	"bufio"
	"bytes"
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"
	"github.com/whiteboxio/flow/pkg/core"
	"github.com/whiteboxio/flow/pkg/metrics"
	evio_rcv "github.com/whiteboxio/flow/pkg/receiver/evio"
)

var (
	ErrMalformedDgram = fmt.Errorf("Malformed datagram")
	ErrEmptyBody      = fmt.Errorf("Empty message body")
)

type UDP struct {
	Name string
	conn *net.UDPConn
	*core.Connector
}

func New(name string, params core.Params) (core.Link, error) {
	udpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("UDP receiver parameters are missing bind_addr")
	}

	if backend, ok := params["backend"]; ok {
		switch backend {
		case "evio":
			log.Info("Instantiating Evio backend for UDP receiver")
			params["listeners"] = []interface{}{
				"udp://" + params["bind_addr"].(string),
			}
			return evio_rcv.New(name, params)
		case "std":
		default:
			return nil, fmt.Errorf("Unknown backend: %s", backend)
		}
	}

	log.Info("Instantiating standard backend for UDP receiver")

	udp := &UDP{
		name,
		nil,
		core.NewConnector(),
	}

	addr, addrErr := net.ResolveUDPAddr("udp", udpAddr.(string))
	if addrErr != nil {
		return nil, addrErr
	}

	conn, connErr := net.ListenUDP("udp", addr)
	if connErr != nil {
		return nil, connErr
	}

	udp.conn = conn

	go udp.recv()

	return udp, nil
}

func (udp *UDP) recv() {
	buf := bufio.NewReader(udp.conn)
	for {
		data, err := buf.ReadBytes('\n')
		metrics.GetCounter("receiver.udp.received").Inc(1)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && (nerr.Temporary() || nerr.Timeout()) {
				metrics.GetCounter("receiver.udp.failed").Inc(1)
				continue
			}
			return
		}

		data = bytes.TrimRight(data, "\r\n")
		msg := core.NewMessage(data)

		if sendErr := udp.Send(msg); sendErr != nil {
			log.Errorf("UDP failed to accept message: %s", sendErr.Error())
		} else {
			metrics.GetCounter("receiver.udp.sent").Inc(1)
		}

	}
}
