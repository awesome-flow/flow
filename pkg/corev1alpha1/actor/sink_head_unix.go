package actor

import (
	"fmt"
	"net"
	"time"
)

const (
	UnixConnTimeout = 5 * time.Second
)

type UnixConnBuilder func(*net.UnixAddr, time.Duration) (net.Conn, error)

var DefaultUnixConnBuilder = func(unixaddr *net.UnixAddr, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("unix", unixaddr.String(), timeout)
}

type SinkHeadUnix struct {
	addr        *net.UnixAddr
	conn        net.Conn
	connbuilder UnixConnBuilder

	ConnectTimeout time.Duration
	WriteTimeout   time.Duration
}

var _ (SinkHead) = (*SinkHeadUnix)(nil)

func NewSinkHeadUnix(unixaddr *net.UnixAddr) (*SinkHeadUnix, error) {
	return &SinkHeadUnix{
		addr:           unixaddr,
		connbuilder:    DefaultUnixConnBuilder,
		ConnectTimeout: UnixConnTimeout,
	}, nil
}

func (h *SinkHeadUnix) Connect() error {
	conn, err := h.connbuilder(h.addr, h.ConnectTimeout)
	if err != nil {
		return err
	}
	h.conn = conn

	return nil
}

func (h *SinkHeadUnix) Start() error {
	return nil
}

func (h *SinkHeadUnix) Stop() error {
	if h.conn != nil {
		return h.conn.Close()
	}
	return nil
}

func (h *SinkHeadUnix) Write(data []byte) (int, error, bool) {
	if h.conn == nil {
		return 0, fmt.Errorf("unix sink head conn is nil"), true
	}
	l := len(data)
	buf := make([]byte, l+2)
	copy(buf, data)
	copy(buf[l:], []byte("\r\n"))
	n, err := h.conn.Write(buf)
	rec := false
	if err != nil {
		h.conn = nil
		rec = true
	}

	return n, err, rec
}
