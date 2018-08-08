package sink

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/whiteboxio/flow/pkg/core"

	"github.com/cenk/backoff"
)

const (
	UdpConnTimeout   = 1 * time.Second
	UdpWriteDeadline = 1 * time.Second
)

type UDP struct {
	Name string
	addr string
	conn net.Conn
	*core.Connector
	*sync.Mutex
}

func NewUDP(name string, params core.Params) (core.Link, error) {
	udpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("UDP sink parameters are missing bind_addr")
	}
	udp := &UDP{
		name, udpAddr.(string), nil, core.NewConnector(), &sync.Mutex{},
	}
	go udp.connect()

	return udp, nil
}

func (udp *UDP) connect() {
	udp.Lock()
	defer udp.Unlock()
	udp.conn = nil
	bckSub := func() error {
		//tell.Infof("Connecting to %s", udp.addr)
		conn, connErr := net.DialTimeout("udp4", udp.addr, UdpConnTimeout)
		if connErr != nil {
			tell.Warnf("Unable to connect to %s: %s", udp.addr, connErr.Error())
			return connErr
		}
		udp.conn = conn
		return nil
	}
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0
	backoff.RetryNotify(bckSub, b, func(err error, dur time.Duration) {
		tell.Warnf("Failed to establish a UDP connection to %s because: %s. "+
			"Next retry in %s", udp.addr, err.Error(), dur)
	})
}

func (udp *UDP) Recv(msg *core.Message) error {
	bmetrics.GetOrRegisterCounter("sink", "udp", "received").Inc(1)
	if udp.conn == nil {
		bmetrics.GetOrRegisterCounter("sink", "udp", "no_connection").Inc(1)
		return msg.AckFailed()
	}
	udp.Lock()
	defer udp.Unlock()
	udp.conn.SetDeadline(time.Now().Add(UdpWriteDeadline))
	if _, err := udp.conn.Write(msg.Payload); err != nil {
		bmetrics.GetOrRegisterCounter("sink", "udp", "failed").Inc(1)
		go udp.connect()
		return msg.AckFailed()
	} else {
		bmetrics.GetOrRegisterCounter("sink", "udp", "sent").Inc(1)
		//bmetrics.GetOrRegisterCounter("sink", "udp", "sent_bytes").Inc(int64(n))
	}
	return msg.AckDone()
}

func (udp *UDP) ConnectTo(core.Link) error {
	panic("UDP sink is not supposed to be connnected")
}
