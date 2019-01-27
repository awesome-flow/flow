package sink

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"text/template"
	"time"

	"github.com/cenk/backoff"
	log "github.com/sirupsen/logrus"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/devenv"
	"github.com/awesome-flow/flow/pkg/metrics"
)

const (
	TcpConnTimeout   = 2 * time.Second
	TcpWriteDeadline = 200 * time.Millisecond
)

type TCP struct {
	Name string
	addr *net.TCPAddr
	conn net.Conn
	*core.Connector
	*sync.Mutex
}

func New(name string, params core.Params, context *core.Context) (core.Link, error) {
	if _, ok := params["bind_addr"]; !ok {
		return nil, fmt.Errorf("TCP sink parameters are missing bind_addr")
	}

	tcpaddr, err := net.ResolveTCPAddr("tcp", params["bind_addr"].(string))
	if err != nil {
		return nil, err
	}

	tcp := &TCP{
		name, tcpaddr, nil, core.NewConnector(), &sync.Mutex{},
	}
	tcp.OnSetUp(tcp.SetUp)
	tcp.OnTearDown(tcp.TearDown)

	return tcp, nil
}

func (tcp *TCP) SetUp() error {
	go tcp.connect()
	return nil
}

func (tcp *TCP) TearDown() error {
	if tcp.conn == nil {
		return fmt.Errorf("tcp connection is empty")
	}
	return tcp.conn.Close()
}

func (tcp *TCP) connect() {
	tcp.Lock()
	defer tcp.Unlock()
	tcp.conn = nil
	bckSub := func() error {
		conn, connErr := net.DialTimeout("tcp", tcp.addr.String(), TcpConnTimeout)
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
	if _, err := tcp.conn.Write(append(msg.Payload(), '\r', '\n')); err != nil {
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

func (tcp *TCP) DevEnv(context *devenv.Context) ([]devenv.Fragment, error) {
	dockercompose, err := template.New("tcp-sink-docker-compose").Parse(`
  tcp_rcv_{{.Port}}:
    image: flow/tcp_server
    ports:
      - "{{.Port}}:{{.Port}}"
    environment:
      TCP_SERVER_PORT: {{.Port}}
`)

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	data := struct{ Port int }{Port: tcp.addr.Port}
	if err := dockercompose.Execute(&buf, data); err != nil {
		return nil, err
	}

	return []devenv.Fragment{
		devenv.DockerComposeFragment(buf.String()),
	}, nil
}
