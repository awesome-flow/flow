package receiver

import (
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"
	"github.com/whiteboxio/flow/pkg/core"
	"github.com/whiteboxio/flow/pkg/metrics"

	"github.com/tidwall/evio"
)

const (
	MaxDatagramPayloadSize = 65536
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

func NewUDP(name string, params core.Params) (core.Link, error) {

	if lib, ok := params["lib"]; ok {
		if lib.(string) == "evio" {
			return NewEvioUDP(name, params)
		}
		log.Errorf("Unable to detect UDP backend library: %s", lib)
	}

	udpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("UDP receiver parameters are missing bind_addr")
	}
	udp := &UDP{name, nil, core.NewConnector()}

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

func NewEvioUDP(name string, params core.Params) (core.Link, error) {
	udp := &UDP{name, nil, core.NewConnector()}

	udpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("UDP receiver parameters are missing bind_addr")
	}

	log.Infof("Starting a new evio handler")

	var events evio.Events
	events.Data = func(connId int, in []byte) (out []byte, action evio.Action) {
		msg := core.NewMessage(nil, in)
		if sendErr := udp.Send(msg); sendErr != nil {
			log.Errorf("UDP failed to accept message: %s", sendErr.Error())
		} else {
			metrics.GetCounter("receiver.udp.sent").Inc(1)
		}
		return
	}
	go func() {
		if err := evio.Serve(events, "udp://"+udpAddr.(string)); err != nil {
			log.Errorf("Unable to connect evio listener: %s", err)
		}
	}()

	return udp, nil
}

func (udp *UDP) recv() {
	buf := make([]byte, MaxDatagramPayloadSize)
	for {
		n, _, err := udp.conn.ReadFromUDP(buf)
		metrics.GetCounter("receiver.udp.received").Inc(1)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && (nerr.Temporary() || nerr.Timeout()) {
				metrics.GetCounter("receiver.udp.failed").Inc(1)
				continue
			}
			return
		}

		msg := core.NewMessage(nil, buf[:n])

		if sendErr := udp.Send(msg); sendErr != nil {
			log.Errorf("UDP failed to accept message: %s", sendErr.Error())
		} else {
			metrics.GetCounter("receiver.udp.sent").Inc(1)
		}
	}
}
