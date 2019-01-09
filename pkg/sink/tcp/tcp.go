package sink

import (
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/metrics"

	"github.com/cenk/backoff"
)

const (
	TcpConnTimeout   = 2 * time.Second
	TcpWriteDeadline = 200 * time.Millisecond
)

type TCP struct {
	Name string
	addr string
	conn net.Conn
	*core.Connector
	*sync.Mutex
}

func New(name string, params core.Params, context *core.Context) (core.Link, error) {
	tcpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("TCP sink parameters are missing bind_addr")
	}
	tcp := &TCP{
		name, tcpAddr.(string), nil, core.NewConnector(), &sync.Mutex{},
	}
	go tcp.connect()
	return tcp, nil
}

func (tcp *TCP) connect() {
	tcp.Lock()
	defer tcp.Unlock()
	tcp.conn = nil
	bckSub := func() error {
		//tell.Infof("Connecting to tcp://%s", tcp.addr)
		conn, connErr := net.DialTimeout("tcp4", tcp.addr, TcpConnTimeout)
		if connErr != nil {
			log.Warnf("Unable to connect to %s: %s", tcp.addr, connErr.Error())
			return connErr
		}
		tcp.conn = conn
		return nil
	}
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0
	backoff.RetryNotify(bckSub, b, func(err error, dur time.Duration) {
		log.Warnf("Failed to establish a TCP connection to %s because: %s. "+
			"Next retry in %s", tcp.addr, err.Error(), dur)
	})
}

func (tcp *TCP) Recv(msg *core.Message) error {
	metrics.GetCounter("sink.tcp.msg.received").Inc(1)
	if tcp.conn == nil {
		metrics.GetCounter("sink.tcp.msg.no_connection").Inc(1)
		return msg.AckFailed()
	}
	tcp.Lock()
	defer tcp.Unlock()
	tcp.conn.SetDeadline(time.Now().Add(TcpWriteDeadline))
	if _, err := tcp.conn.Write(append(msg.Payload, '\r', '\n')); err != nil {
		metrics.GetCounter("sink.tcp.msg.failed").Inc(1)
		log.Warnf("Failed to send TCP packet to %s: %s", tcp.addr, err.Error())
		go tcp.connect()
		return msg.AckFailed()
	} else {
		metrics.GetCounter("sink.tcp.msg.sent").Inc(1)
	}

	return msg.AckDone()
}

func (tcp *TCP) ConnectTo(core.Link) error {
	panic("TCP sink is not supposed to be connnected")
}

func (tcp *TCP) String() string {
	return tcp.Name
}
