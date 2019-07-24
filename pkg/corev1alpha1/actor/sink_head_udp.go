package actor

import (
	"fmt"
	"net"
	"time"
)

const (
	UDPConnTimeout = 5 * time.Second
)

type UDPConnBuilder func(*net.UDPAddr, time.Duration) (net.Conn, error)

var DefaultUDPConnBuilder = func(udpaddr *net.UDPAddr, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("udp", udpaddr.String(), timeout)
}

type SinkHeadUDP struct {
	addr        *net.UDPAddr
	conn        net.Conn
	connbuilder UDPConnBuilder

	ConnectTimeout time.Duration
	WriteTimeout   time.Duration
}

var _ (SinkHead) = (*SinkHeadUDP)(nil)

func NewSinkHeadUDP(udpaddr *net.UDPAddr) (*SinkHeadUDP, error) {
	return &SinkHeadUDP{
		addr:           udpaddr,
		connbuilder:    DefaultUDPConnBuilder,
		ConnectTimeout: UDPConnTimeout,
	}, nil
}

func (h *SinkHeadUDP) Connect() error {
	conn, err := h.connbuilder(h.addr, h.ConnectTimeout)
	if err != nil {
		return err
	}
	h.conn = conn

	return nil
}

func (h *SinkHeadUDP) Start() error {
	return nil
}

func (h *SinkHeadUDP) Stop() error {
	if h.conn != nil {
		return h.conn.Close()
	}
	return nil
}

func (h *SinkHeadUDP) Write(data []byte) (int, error, bool) {
	if h.conn == nil {
		return 0, fmt.Errorf("udp sink head conn is nil"), true
	}
	n, err := h.conn.Write(data)
	rec := false
	if err != nil {
		h.conn = nil
		rec = true
	}

	return n, err, rec
}
