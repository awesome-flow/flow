package receiver

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/metrics"
	eviorcv "github.com/awesome-flow/flow/pkg/receiver/evio"
	"github.com/awesome-flow/flow/pkg/types"
)

const (
	ConnReadTimeout  = 50 * time.Millisecond
	ConnWriteTimeout = 50 * time.Millisecond
)

const (
	replyModeSilent replyMode = iota
	replyModeTalkative
)

const (
	TcpMetricsPref = "receiver.tcp.msg"

	TcpMetricsConnFailed = "receiver.tcp.conn.failed"
	TcpMetricsConnOpnd   = "receiver.tcp.conn.opened"
	TcpMetricsConnClosed = "receiver.tcp.conn.closed"

	TcpMetricsMsgFailed  = "receiver.tcp.msg.failed"
	TcpMetricsMsgRcvd    = "receiver.tcp.msg.received"
	TcpMetricsMsgTimeout = "receiver.tcp.msg.timeout"
)

var TcpMetricsMsgStatusMap = map[core.MsgStatus]string{
	core.MsgStatusDone:        TcpMetricsPref + ".sent_ok",
	core.MsgStatusFailed:      TcpMetricsPref + ".sent_failed",
	core.MsgStatusInvalid:     TcpMetricsPref + ".sent_invalid",
	core.MsgStatusPartialSend: TcpMetricsPref + ".sent_part",
	core.MsgStatusTimedOut:    TcpMetricsPref + ".sent_timeout",
	core.MsgStatusThrottled:   TcpMetricsPref + ".sent_throttled",
	core.MsgStatusUnroutable:  TcpMetricsPref + ".sent_unroutable",
}

var MsgStatusToTcpResp = map[core.MsgStatus][]byte{
	core.MsgStatusDone:        TcpRespOk,
	core.MsgStatusPartialSend: TcpRespPsnt,
	core.MsgStatusInvalid:     TcpRespInvd,
	core.MsgStatusFailed:      TcpRespFail,
	core.MsgStatusTimedOut:    TcpRespTime,
	core.MsgStatusUnroutable:  TcpRespUnrt,
	core.MsgStatusThrottled:   TcpRespThrt,
}

var (
	TcpRespFail = []byte("FAILED\r\n")
	TcpRespInvd = []byte("INVALID\r\n")
	TcpRespPsnt = []byte("PARTSENT\r\n")
	TcpRespOk   = []byte("OK\r\n")
	TcpRespTime = []byte("TIMEOUT\r\n")
	TcpRespThrt = []byte("THROTTLED\r\n")
	TcpRespUnrt = []byte("UNROUTABLE\r\n")

	ErrMalformedPacket = fmt.Errorf("Malformed packet")
	ErrEmptyBody       = fmt.Errorf("Empty message body")

	MsgSendTimeout = 100 * time.Millisecond
)

type replyMode uint8

type TCP struct {
	Name string
	mode replyMode
	addr *net.TCPAddr
	srv  net.Listener
	*core.Connector
}

func New(name string, params types.Params, context *core.Context) (core.Link, error) {
	bindaddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("TCP receiver is missing bind_addr")
	}

	tcpaddr, err := net.ResolveTCPAddr("tcp", bindaddr.(string))
	if err != nil {
		return nil, fmt.Errorf("Failed to parse TCP bind_addr: %s", err)
	}

	mode := replyModeTalkative
	if alterMode, ok := params["mode"]; ok {
		switch alterMode {
		case "silent":
			mode = replyModeSilent
		case "talkative":
			mode = replyModeTalkative
		}
	}
	if backend, ok := params["backend"]; ok {
		switch backend {
		case "evio":
			log.Info("Instantiating Evio backend for TCP receiver")
			params["listeners"] = []interface{}{
				"tcp://" + params["bind_addr"].(string),
			}
			return eviorcv.New(name, params, context)
		case "std":
		default:
			return nil, fmt.Errorf("Unknown backend: %s", backend)
		}
	}

	log.Info("Instantiating standard backend for TCP receiver")

	tcp := &TCP{
		name + "@" + bindaddr.(string),
		mode,
		tcpaddr,
		nil,
		core.NewConnector(),
	}

	tcp.OnSetUp(tcp.SetUp)
	tcp.OnTearDown(tcp.TearDown)

	return tcp, nil
}

func (tcp *TCP) SetUp() error {
	srv, err := net.Listen("tcp", tcp.addr.String())
	if err != nil {
		return err
	}
	tcp.srv = srv
	go tcp.handleListener()

	return nil
}

func (tcp *TCP) TearDown() error {
	if tcp.srv == nil {
		return fmt.Errorf("TCP listener is empty on tear down")
	}
	return tcp.srv.Close()
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
	metrics.GetCounter(TcpMetricsConnOpnd).Inc(1)

	for {
		conn.SetReadDeadline(time.Now().Add(ConnReadTimeout))
		data, err := reader.ReadBytes('\n')

		if len(data) == 0 {
			break
		}

		metrics.GetCounter(TcpMetricsMsgRcvd).Inc(1)

		if err != nil && err != io.EOF {
			log.Errorf("TCP receiver failed to read data: %s", err)
			metrics.GetCounter(TcpMetricsConnFailed).Inc(1)
			tcp.replyWith(conn, TcpRespInvd)
			conn.Close()
			metrics.GetCounter(TcpMetricsConnClosed).Inc(1)
			return
		}

		msg := core.NewMessage(bytes.TrimRight(data, "\r\n"))

		if sendErr := tcp.Send(msg); sendErr != nil {
			metrics.GetCounter(TcpMetricsMsgFailed).Inc(1)
			log.Errorf("Failed to send message: %s", sendErr)
			tcp.replyWith(conn, TcpRespFail)
			continue
		}

		select {
		case s := <-msg.AckCh():
			metrics.GetCounter(TcpMetricsMsgStatusMap[s]).Inc(1)
			conn.SetWriteDeadline(time.Now().Add(ConnWriteTimeout))
			tcp.replyWith(conn, MsgStatusToTcpResp[s])
		case <-time.After(MsgSendTimeout):
			metrics.GetCounter(TcpMetricsMsgTimeout).Inc(1)
			tcp.replyWith(conn, TcpRespTime)
		}

		if err == io.EOF {
			break
		}
	}
	metrics.GetCounter(TcpMetricsConnClosed).Inc(1)
	conn.Close()
}

func (tcp *TCP) replyWith(conn net.Conn, reply []byte) {
	if tcp.mode == replyModeSilent {
		return
	}
	conn.SetWriteDeadline(time.Now().Add(ConnWriteTimeout))
	conn.Write(reply)
}
