package receiver

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"runtime"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/metrics"
	evio_rcv "github.com/awesome-flow/flow/pkg/receiver/evio"
	"github.com/awesome-flow/flow/pkg/types"
	log "github.com/sirupsen/logrus"
)

const (
	UpdMetricsConnFailed = "receiver.udp.conn.failed"

	UdpMetricsMsgReceived = "receiver.udp.msg.received"
	UdpMetricsMsgSent     = "receiver.udp.msg.sent"
	UpdMetricsMsgFailed   = "receiver.udp.msg.failed"
)

var (
	ErrMalformedDgram = fmt.Errorf("Malformed datagram")
	ErrEmptyBody      = fmt.Errorf("Empty message body")
)

type UDP struct {
	Name string
	addr *net.UDPAddr
	conn *net.UDPConn
	*core.Connector
}

func New(name string, params types.Params, context *core.Context) (core.Link, error) {
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
			return evio_rcv.New(name, params, context)
		case "std":
		default:
			return nil, fmt.Errorf("Unknown backend: %s", backend)
		}
	}

	log.Info("Instantiating standard backend for UDP receiver")

	addr, addrErr := net.ResolveUDPAddr("udp", udpAddr.(string))
	if addrErr != nil {
		return nil, addrErr
	}

	udp := &UDP{
		name,
		addr,
		nil,
		core.NewConnector(),
	}
	udp.OnSetUp(udp.SetUp)
	udp.OnTearDown(udp.TearDown)

	return udp, nil
}

func (udp *UDP) SetUp() error {
	conn, err := net.ListenUDP("udp", udp.addr)
	if err != nil {
		return err
	}

	udp.conn = conn

	for i := 0; i < runtime.GOMAXPROCS(-1); i++ {
		go udp.recv()
	}

	return nil
}

func (udp *UDP) TearDown() error {
	if udp.conn == nil {
		return fmt.Errorf("udp listener is empty")
	}

	return udp.conn.Close()
}

func (udp *UDP) recv() {
	buf := bufio.NewReader(udp.conn)
	for {
		data, err := buf.ReadBytes('\n')
		metrics.GetCounter(UdpMetricsMsgReceived).Inc(1)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && (nerr.Temporary() || nerr.Timeout()) {
				metrics.GetCounter(UpdMetricsMsgFailed).Inc(1)
				continue
			} else {
				metrics.GetCounter(UpdMetricsConnFailed).Inc(1)
				return
			}
		}

		msg := core.NewMessage(bytes.TrimRight(data, "\r\n"))

		if sendErr := udp.Send(msg); sendErr != nil {
			log.Errorf("UDP failed to accept message: %s", sendErr.Error())
		} else {
			metrics.GetCounter(UdpMetricsMsgSent).Inc(1)
		}

	}
}
