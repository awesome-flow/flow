package sink

import (
	"bytes"
	"fmt"
	"html/template"
	"net"
	"sync"
	"time"

	"github.com/awesome-flow/flow/pkg/devenv"
	"github.com/awesome-flow/flow/pkg/types"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/metrics"
	log "github.com/sirupsen/logrus"

	"github.com/cenk/backoff"
)

const (
	UdpConnTimeout   = 1 * time.Second
	UdpWriteDeadline = 1 * time.Second
)

type UDP struct {
	Name string
	addr *net.UDPAddr
	conn net.Conn
	*core.Connector
	*sync.Mutex
}

func New(name string, params types.Params, context *core.Context) (core.Link, error) {
	if _, ok := params["bind_addr"]; !ok {
		return nil, fmt.Errorf("UDP sink parameters are missing bind_addr")
	}

	udpaddr, err := net.ResolveUDPAddr("udp", params["bind_addr"].(string))
	if err != nil {
		return nil, err
	}

	udp := &UDP{
		name, udpaddr, nil, core.NewConnector(), &sync.Mutex{},
	}

	udp.OnSetUp(udp.SetUp)
	udp.OnTearDown(udp.TearDown)

	return udp, nil
}

func (udp *UDP) SetUp() error {
	go udp.connect()
	return nil
}

func (udp *UDP) TearDown() error {
	if udp.conn == nil {
		return fmt.Errorf("udp conn is empty")
	}
	return udp.conn.Close()
}

func (udp *UDP) connect() {
	udp.Lock()
	defer udp.Unlock()
	udp.conn = nil
	bckSub := func() error {
		conn, connErr := net.DialTimeout("udp", udp.addr.String(), UdpConnTimeout)
		if connErr != nil {
			log.Warnf("Unable to connect to %s: %s", udp.addr, connErr.Error())
			return connErr
		}
		udp.conn = conn
		return nil
	}
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0
	backoff.RetryNotify(bckSub, b, func(err error, dur time.Duration) {
		log.Warnf("Failed to establish a UDP connection to %s because: %s. "+
			"Next retry in %s", udp.addr, err.Error(), dur)
	})
}

func (udp *UDP) Recv(msg *core.Message) error {
	metrics.GetCounter("sink.udp.received").Inc(1)
	if udp.conn == nil {
		metrics.GetCounter("sink.udp.no_connection").Inc(1)
		return msg.AckFailed()
	}
	udp.Lock()
	defer udp.Unlock()
	udp.conn.SetDeadline(time.Now().Add(UdpWriteDeadline))
	if _, err := udp.conn.Write(msg.Payload()); err != nil {
		metrics.GetCounter("sink.udp.failed").Inc(1)
		go udp.connect()
		return msg.AckFailed()
	} else {
		metrics.GetCounter("sink.udp.sent").Inc(1)
	}
	return msg.AckDone()
}

func (udp *UDP) ConnectTo(core.Link) error {
	panic("UDP sink is not supposed to be connnected")
}

func (udp *UDP) DevEnv(context *devenv.Context) ([]devenv.Fragment, error) {
	dockercompose, err := template.New("udp-sink-docker-compose").Parse(`
  udp_rcv_{{.Port}}
	image: flow/udp_server
	ports:
	  - "{{.Port}}:{{.Port}}/udp"
	environment:
      UDP_SERVER_PORT: {{.Port}}
`)

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	data := struct{ Port int }{Port: udp.addr.Port}
	if err := dockercompose.Execute(&buf, data); err != nil {
		return nil, err
	}

	return []devenv.Fragment{
		devenv.DockerComposeFragment(buf.String()),
	}, nil
}
