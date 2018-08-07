package sink

import (
	"booking/bmetrics"
	"booking/msgrelay/flow"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cenk/backoff"
	"gitlab.booking.com/go/tell"
)

const (
	TcpConnTimeout   = 2 * time.Second
	TcpWriteDeadline = 200 * time.Millisecond
)

type TCP struct {
	Name string
	addr string
	conn net.Conn
	*flow.Connector
	*sync.Mutex
}

func NewTCP(name string, params flow.Params) (flow.Link, error) {
	tcpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("TCP sink parameters are missing bind_addr")
	}
	tcp := &TCP{
		name, tcpAddr.(string), nil, flow.NewConnector(), &sync.Mutex{},
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
			tell.Warnf("Unable to connect to %s: %s", tcp.addr, connErr.Error())
			return connErr
		}
		tcp.conn = conn
		return nil
	}
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0
	backoff.RetryNotify(bckSub, b, func(err error, dur time.Duration) {
		tell.Warnf("Failed to establish a TCP connection to %s because: %s. "+
			"Next retry in %s", tcp.addr, err.Error(), dur)
	})
}

func (tcp *TCP) Recv(msg *flow.Message) error {
	bmetrics.GetOrRegisterCounter("sink", "tcp", "received").Inc(1)
	if tcp.conn == nil {
		bmetrics.GetOrRegisterCounter("sink", "tcp", "no_connection").Inc(1)
		return msg.AckFailed()
	}
	tcp.Lock()
	defer tcp.Unlock()
	tcp.conn.SetDeadline(time.Now().Add(TcpWriteDeadline))
	if _, err := tcp.conn.Write(msg.Payload); err != nil {
		bmetrics.GetOrRegisterCounter("sink", "tcp", "failed").Inc(1)
		tell.Warnf("Failed to send TCP packet to %s: %s", tcp.addr, err.Error())
		go tcp.connect()
		return msg.AckFailed()
	} else {
		bmetrics.GetOrRegisterCounter("sink", "tcp", "sent").Inc(1)
		// bmetrics.GetOrRegisterCounter("sink", "tcp", "sent_bytes").Inc(int64(n))
	}

	return msg.AckDone()
}

func (tcp *TCP) ConnectTo(flow.Link) error {
	panic("TCP sink is not supposed to be connnected")
}
