package receiver

import (
	"booking/bmetrics"
	"booking/msgrelay/flow"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/facebookgo/grace/gracenet"
	"gitlab.booking.com/go/tell"
)

const (
	MaxTCPBufSize = 65536
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
	*flow.Connector
}

func NewTCP(name string, params flow.Params) (flow.Link, error) {
	tcpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("TCP receiver parameters are missing bind_addr")
	}
	net := &gracenet.Net{}
	srv, err := net.Listen("tcp", tcpAddr.(string))
	if err != nil {
		return nil, err
	}
	tcp := &TCP{name + "@" + tcpAddr.(string), srv, flow.NewConnector()}
	go tcp.handleListener()

	return tcp, nil
}

func (tcp *TCP) handleListener() {
	for {
		//tell.Info("Waiting for TCP connections")
		conn, err := tcp.srv.Accept()
		//tell.Info("Listening from a new TCP connection")
		if err != nil {
			tell.Errorf("TCP server failed to accept connection: %s", err.Error())
			continue
		}
		go tcp.handleConnection(conn)
	}
}

func (tcp *TCP) handleConnection(conn net.Conn) {
	bmetrics.GetOrRegisterCounter("receiver", "tcp", "received").Inc(1)
	buf := make([]byte, MaxTCPBufSize)
	len, err := conn.Read(buf)
	if err != nil {
		bmetrics.GetOrRegisterCounter("receiver", "tcp", "failed").Inc(1)
		tell.Errorf("Failed to read TCP message: %s", err.Error())
		conn.Write([]byte(TCP_RESP_INVD))
		return
	}

	msg := flow.NewMessage(nil, buf[:len])

	if sendErr := tcp.Send(msg); sendErr != nil {
		bmetrics.GetOrRegisterCounter("receiver", "tcp", "failed").Inc(1)
		tell.Errorf("Failed to send message: %s", sendErr.Error())
		conn.Write([]byte(TCP_RESP_FAIL))
		return
	}

	if !msg.IsSync() {
		bmetrics.GetOrRegisterCounter("receiver", "tcp", "accepted").Inc(1)
		conn.Write([]byte(TCP_RESP_ACPT))
		return
	}

	select {
	case s := <-msg.GetAckCh():
		bmetrics.GetOrRegisterCounter(
			"receiver", "tcp", "sent_"+strings.ToLower(string(status2resp(s)))).Inc(1)
		conn.Write(status2resp(s))
	case <-time.After(TcpMsgSendTimeout):
		conn.Write([]byte(TCP_RESP_TIME))
	}
}

func status2resp(s flow.MsgStatus) []byte {
	switch s {
	case flow.MsgStatusDone:
		return []byte(TCP_RESP_SENT)
	case flow.MsgStatusPartialSend:
		return []byte(TCP_RESP_PSNT)
	case flow.MsgStatusInvalid:
		return []byte(TCP_RESP_INVD)
	case flow.MsgStatusFailed:
		return []byte(TCP_RESP_FAIL)
	case flow.MsgStatusTimedOut:
		return []byte(TCP_RESP_TIME)
	case flow.MsgStatusUnroutable:
		return []byte(TCP_RESP_UNRT)
	case flow.MsgStatusThrottled:
		return []byte(TCP_RESP_THRT)
	default:
		return []byte("OlegS made a mistake, this should not happen")
	}
}