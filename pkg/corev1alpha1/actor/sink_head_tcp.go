package actor

import (
	"fmt"
	"net"
	"time"
)

const (
	TCPConnTimeout = 5 * time.Second
)

type TCPConnBuilder func(*net.TCPAddr, time.Duration) (net.Conn, error)

var DefaultTCPConnBuilder = func(tcpaddr *net.TCPAddr, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", tcpaddr.String(), timeout)
}

type SinkHeadTCP struct {
	addr        *net.TCPAddr
	conn        net.Conn
	connbuilder TCPConnBuilder

	ConnectTimeout time.Duration
	WriteTimeout   time.Duration
}

var _ (SinkHead) = (*SinkHeadTCP)(nil)

func NewSinkHeadTCP(tcpaddr *net.TCPAddr) (*SinkHeadTCP, error) {
	return &SinkHeadTCP{
		addr:           tcpaddr,
		connbuilder:    DefaultTCPConnBuilder,
		ConnectTimeout: TCPConnTimeout,
	}, nil
}

func (h *SinkHeadTCP) Connect() error {
	conn, err := h.connbuilder(h.addr, h.ConnectTimeout)
	if err != nil {
		return err
	}
	h.conn = conn

	return nil
}

func (h *SinkHeadTCP) Start() error {
	return h.Connect()
}

func (h *SinkHeadTCP) Stop() error {
	if h.conn != nil {
		return h.conn.Close()
	}
	return nil
}

func (h *SinkHeadTCP) Write(data []byte) (int, error, bool) {
	if h.conn == nil {
		return 0, fmt.Errorf("tcp sink head conn is nil"), true
	}
	l := len(data)
	buf := make([]byte, l+2)
	copy(buf, data)
	copy(buf[l:], []byte("\r\n"))
	rec := false
	n, err := h.conn.Write(buf)
	if err != nil {
		rec = true
		h.conn = nil
	}

	return n, err, rec
}
