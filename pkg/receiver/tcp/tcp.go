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

	ConnReadTimeout  = 1 * time.Second
	ConnWriteTimeout = 1 * time.Second
)

var (
	TcpRespAcpt = []byte("ACCEPTED")
	TcpRespSent = []byte("SENT")
	TcpRespPsnt = []byte("PART_SENT")
	TcpRespFail = []byte("FAILED")
	TcpRespInvd = []byte("INVALID")
	TcpRespTime = []byte("TIMEOUT")
	TcpRespUnrt = []byte("UNROUTABLE")
	TcpRespThrt = []byte("THROTTLED")

	ErrMalformedPacket = fmt.Errorf("Malformed packet")
	ErrEmptyBody       = fmt.Errorf("Empty message body")

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
		switch backend {
		case "evio":
			log.Info("Instantiating Evio backend for TCP receiver")
			params["listeners"] = []interface{}{
				"tcp://" + params["bind_addr"].(string),
			}
			return evio_rcv.New(name, params)
		case "std":
			log.Info("Instantiating standard backend for TCP receiver")
		default:
			return nil, fmt.Errorf("Unknown backend: %s", backend)
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
		conn.SetReadDeadline(time.Now().Add(ConnReadTimeout))
		data, err := reader.ReadBytes('\n')

		if len(data) == 0 {
			break
		}

		metrics.GetCounter("receiver.tcp.msg.received").Inc(1)

		if err != nil && err != io.EOF {
			log.Errorf("TCP receiver failed to read data: %s", err)
			metrics.GetCounter("receiver.tcp.conn.failed").Inc(1)
			conn.SetWriteDeadline(time.Now().Add(ConnWriteTimeout))
			conn.Write(TcpRespInvd)
			conn.Close()
			metrics.GetCounter("receiver.tcp.conn.closed").Inc(1)
			return
		}

		msg := core.NewMessage(data)

		if sendErr := tcp.Send(msg); sendErr != nil {
			metrics.GetCounter("receiver.tcp.msg.failed").Inc(1)
			log.Errorf("Failed to send message: %s", sendErr)
			conn.SetWriteDeadline(time.Now().Add(ConnWriteTimeout))
			conn.Write(TcpRespFail)
			continue
		}

		sync, ok := msg.GetMeta("sync")
		isSync := ok && (sync.(string) == "true" || sync.(string) == "1")
		if !isSync {
			metrics.GetCounter("receiver.tcp.msg.accepted").Inc(1)
			conn.SetWriteDeadline(time.Now().Add(ConnWriteTimeout))
			conn.Write(TcpRespAcpt)
			continue
		}

		select {
		case s := <-msg.GetAckCh():
			metrics.GetCounter(
				"receiver.tcp.msg.sent_" + strings.ToLower(string(status2resp(s)))).Inc(1)
			conn.SetWriteDeadline(time.Now().Add(ConnWriteTimeout))
			conn.Write(status2resp(s))
		case <-time.After(TcpMsgSendTimeout):
			metrics.GetCounter("receiver.tcp.msg.timed_out").Inc(1)
			conn.SetWriteDeadline(time.Now().Add(ConnWriteTimeout))
			conn.Write(TcpRespTime)
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
		return TcpRespSent
	case core.MsgStatusPartialSend:
		return TcpRespPsnt
	case core.MsgStatusInvalid:
		return TcpRespInvd
	case core.MsgStatusFailed:
		return TcpRespFail
	case core.MsgStatusTimedOut:
		return TcpRespTime
	case core.MsgStatusUnroutable:
		return TcpRespUnrt
	case core.MsgStatusThrottled:
		return TcpRespThrt
	default:
		return []byte("This should not happen")
	}
}
