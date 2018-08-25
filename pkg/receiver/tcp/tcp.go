package receiver

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/whiteboxio/flow/pkg/core"
	"github.com/whiteboxio/flow/pkg/metrics"
	evio_rcv "github.com/whiteboxio/flow/pkg/receiver/evio"

	"github.com/facebookgo/grace/gracenet"
)

const (
	MaxTCPBufSize = 65536

	CONN_READ_TIMEOUT  = 1 * time.Second
	CONN_WRITE_TIMEOUT = 1 * time.Second
)

const (
	TCP_RESP_ACPT = "ACCEPTED"
	TCP_RESP_SENT = "SENT"
	TCP_RESP_PSNT = "PART_SENT"
	TCP_RESP_FAIL = "FAILED"
	TCP_RESP_INVD = "INVALID"
	TCP_RESP_TIME = "TIMEOUT"
	TCP_RESP_UNRT = "UNROUTABLE"
	TCP_RESP_THRT = "THROTTLED"
)

var (
	ErrMalformedPacket = fmt.Errorf("Malformed packet")
	ErrEmptyBody       = fmt.Errorf("Empty message body")
)

var (
	TcpMsgSendTimeout = 100 * time.Millisecond
)

type TCP struct {
	Name string
	srv  net.Listener
	*core.Connector
}

func New(name string, params core.Params) (core.Link, error) {
	tcpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("TCP receiver parameters are missing bind_addr")
	}
	if backend, ok := params["backend"]; ok {
		if backend == "evio" {
			log.Debug("Instantiating Evio backend for TCP receiver")
			params["listeners"] = []interface{}{
				"tcp://" + params["bind_addr"].(string),
			}
			return evio_rcv.New(name, params)
		}
	}
	net := &gracenet.Net{}
	srv, err := net.Listen("tcp", tcpAddr.(string))
	if err != nil {
		return nil, err
	}
	tcp := &TCP{name + "@" + tcpAddr.(string), srv, core.NewConnector()}
	go tcp.handleListener()

	return tcp, nil
}

func (tcp *TCP) handleListener() {
	for {
		conn, err := tcp.srv.Accept()
		if err != nil {
			log.Errorf("TCP server failed to accept connection: %s", err.Error())
			continue
		}
		log.Infof("Received a new connection from %s", conn.RemoteAddr())
		go tcp.handleConnection(conn)
	}
}

func (tcp *TCP) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)

	metrics.GetCounter("receiver.tcp.conn.opened").Inc(1)

	for {
		conn.SetReadDeadline(time.Now().Add(CONN_READ_TIMEOUT))
		data, err := reader.ReadBytes('\n')

		if len(data) == 0 {
			break
		}

		metrics.GetCounter("receiver.tcp.msg.received").Inc(1)

		if err != nil && err != io.EOF {
			log.Errorf("TCP receiver failed to read data: %s", err)
			metrics.GetCounter("receiver.tcp.conn.failed").Inc(1)
			conn.SetWriteDeadline(time.Now().Add(CONN_WRITE_TIMEOUT))
			conn.Write([]byte(TCP_RESP_INVD))
			conn.Close()
			metrics.GetCounter("receiver.tcp.conn.closed").Inc(1)
			return
		}

		msg := core.NewMessage(core.NewMsgMeta(), data)

		if sendErr := tcp.Send(msg); sendErr != nil {
			metrics.GetCounter("receiver.tcp.msg.failed").Inc(1)
			log.Errorf("Failed to send message: %s", sendErr)
			conn.SetWriteDeadline(time.Now().Add(CONN_WRITE_TIMEOUT))
			conn.Write([]byte(TCP_RESP_FAIL))
			continue
		}

		if !msg.IsSync() {
			metrics.GetCounter("receiver.tcp.msg.accepted").Inc(1)
			conn.SetWriteDeadline(time.Now().Add(CONN_WRITE_TIMEOUT))
			conn.Write([]byte(TCP_RESP_ACPT))
			continue
		}

		select {
		case s := <-msg.GetAckCh():
			metrics.GetCounter(
				"receiver.tcp.msg.sent_" + strings.ToLower(string(status2resp(s)))).Inc(1)
			conn.SetWriteDeadline(time.Now().Add(CONN_WRITE_TIMEOUT))
			conn.Write(status2resp(s))
		case <-time.After(TcpMsgSendTimeout):
			metrics.GetCounter("receiver.tcp.msg.timed_out").Inc(1)
			conn.SetWriteDeadline(time.Now().Add(CONN_WRITE_TIMEOUT))
			conn.Write([]byte(TCP_RESP_TIME))
		}

		if err == io.EOF {
			break
		}
	}
	metrics.GetCounter("receiver.tcp.conn.closed").Inc(1)
	conn.Close()
}

func status2resp(s core.MsgStatus) []byte {
	switch s {
	case core.MsgStatusDone:
		return []byte(TCP_RESP_SENT)
	case core.MsgStatusPartialSend:
		return []byte(TCP_RESP_PSNT)
	case core.MsgStatusInvalid:
		return []byte(TCP_RESP_INVD)
	case core.MsgStatusFailed:
		return []byte(TCP_RESP_FAIL)
	case core.MsgStatusTimedOut:
		return []byte(TCP_RESP_TIME)
	case core.MsgStatusUnroutable:
		return []byte(TCP_RESP_UNRT)
	case core.MsgStatusThrottled:
		return []byte(TCP_RESP_THRT)
	default:
		return []byte("OlegS made a mistake, this should not happen")
	}
}
