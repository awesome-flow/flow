package receiver

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/cenkalti/backoff"
	log "github.com/sirupsen/logrus"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/metrics"
	evio_rcv "github.com/awesome-flow/flow/pkg/receiver/evio"
)

type Unix struct {
	Name     string
	path     string
	listener net.Listener
	*core.Connector
}

const (
	UnixMetricsConnOpened  = "receiver.unix.conn.opened"
	UnixMetricsConnClosed  = "receiver.unix.conn.closed"
	UnixMetricsMsgReceived = "receiver.unix.msg.received"
	UnixMetricsMsgFailed   = "receiver.unix.msg.failed"
	UnixMetricsMsgSendErr  = "receiver.unix.msg.send_err"
	UnixMetricsMsgSent     = "receiver.unix.msg.sent"

	FlowUnixSock = "/tmp/flow.sock"
)

var (
	ErrMalformedUnixgram = fmt.Errorf("Malformed unixgram")
	ErrEmptyBody         = fmt.Errorf("Empty message body")
)

func New(name string, params core.Params, context *core.Context) (core.Link, error) {
	path, ok := params["bind_addr"]
	if !ok {
		path = FlowUnixSock
	}

	if backend, ok := params["backend"]; ok {
		switch backend {
		case "evio":
			log.Info("Instantiating Evio backend for UNIX receiver")
			params["listeners"] = []interface{}{
				"unix://" + path.(string),
			}
			return evio_rcv.New(name, params, context)
		case "std":
		default:
			return nil, fmt.Errorf("Unknown backend: %s", backend)
		}
	}

	log.Info("Instantiating standard backend for UNIX receiver")

	lstnr, err := net.Listen("unix", path.(string))
	if err != nil {
		return nil, err
	}

	ux := &Unix{name, path.(string), lstnr, core.NewConnector()}

	ux.OnSetUp(ux.SetUp)
	ux.OnTearDown(ux.TearDown)

	return ux, nil
}

func (ux *Unix) SetUp() error {
	go func() {
		if err := backoff.Retry(func() error {
			conn, err := ux.listener.Accept()
			if err != nil {
				return err
			}
			go ux.unixRecv(conn)
			return nil
		}, backoff.NewExponentialBackOff()); err != nil {
			// Unrecoverable error, giving up
			panic(err.Error())
		}
	}()
	return nil
}

func (ux *Unix) TearDown() error {
	return ux.listener.Close()
}

func (ux *Unix) unixRecv(conn net.Conn) {
	metrics.GetCounter(UnixMetricsConnOpened).Inc(1)
	reader := bufio.NewReader(conn)
	for {
		data, err := reader.ReadBytes('\n')
		metrics.GetCounter(UnixMetricsMsgReceived).Inc(1)

		if err != nil {
			if err == io.EOF {
				break
			}
			log.Warnf("Unix conn Read failed: %s", err)
			metrics.GetCounter(UnixMetricsMsgFailed).Inc(1)
			if err := ux.Reset(); err != nil {
				panic(err.Error())
			}
			return
		}

		if len(data) == 0 {
			continue
		}
		msg := core.NewMessage(data)

		if sendErr := ux.Send(msg); sendErr != nil {
			metrics.GetCounter(UnixMetricsMsgSendErr).Inc(1)
			log.Errorf("Unix socket failed to send message: %s", sendErr)
		} else {
			metrics.GetCounter(UnixMetricsMsgSent).Inc(1)
		}
	}

	if err := conn.Close(); err != nil {
		log.Errorf("Unix socket connection failed to close: %s", err)
	}
	metrics.GetCounter(UnixMetricsConnClosed).Inc(1)
}
