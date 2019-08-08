package corev1alpha1

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/awesome-flow/flow/pkg/cfg"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/whiteboxio/flow/pkg/core"
)

func newContextWithConfig(config map[string]interface{}) (*Context, error) {
	repo := cfg.NewRepository()
	for k, v := range config {
		if _, err := cfg.NewScalarConfigProvider(
			&types.KeyValue{
				Key:   types.NewKey(k),
				Value: v,
			},
			repo,
			42, // doesn't matter
		); err != nil {
			return nil, err
		}
	}

	ctx, err := NewContext(NewConfig(repo))
	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func eqErr(e1, e2 error) bool {
	if e1 == nil || e2 == nil {
		return e1 == e2
	}
	return e1.Error() == e2.Error()
}

func sts2name(sts core.MsgStatus) string {
	switch sts {
	case core.MsgStatusDone:
		return "MsgStatusDone"
	case core.MsgStatusFailed:
		return "MsgStatusFailed"
	case core.MsgStatusTimedOut:
		return "MsgStatusTimedOut"
	case core.MsgStatusUnroutable:
		return "MsgStatusUnroutable"
	case core.MsgStatusThrottled:
		return "MsgStatusThrottled"
	default:
		return "Unknown"
	}
}

type testAddr struct {
	network string
	address string
}

var _ net.Addr = (*testAddr)(nil)

func newTestAddr(network, address string) *testAddr {
	return &testAddr{
		network: network,
		address: address,
	}
}

func (a *testAddr) Network() string {
	return a.network
}

func (a *testAddr) String() string {
	return fmt.Sprintf("%s://%s", a.network, a.address)
}

type testConn struct {
	buf        []byte
	offset     int
	lock       sync.Mutex
	localaddr  net.Addr
	remoteaddr net.Addr
	closed     bool
}

var _ net.Conn = (*testConn)(nil)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func newTestConn(localaddr, remoteaddr net.Addr) *testConn {
	return &testConn{
		buf:        make([]byte, 0),
		localaddr:  localaddr,
		remoteaddr: remoteaddr,
		closed:     false,
	}
}

func (c *testConn) Read(b []byte) (int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var err error
	l := min(len(b), len(c.buf)-c.offset)
	n := copy(b, c.buf[c.offset:c.offset+l+0])

	c.offset += l
	if c.offset == len(c.buf) {
		err = io.EOF
	}

	return n, err
}

func (c *testConn) Write(b []byte) (int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.buf = make([]byte, len(b))
	n := copy(c.buf, b)

	return n, nil
}

func (c *testConn) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closed = true

	return nil
}

func (c *testConn) LocalAddr() net.Addr {
	return c.localaddr
}

func (c *testConn) RemoteAddr() net.Addr {
	return c.remoteaddr
}

func (c *testConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *testConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *testConn) SetWriteDeadline(t time.Time) error {
	return nil
}

type testResponseWriter struct {
	headers map[string][]string
	status  int
	bytes.Buffer
}

var _ http.ResponseWriter = (*testResponseWriter)(nil)

func (rw *testResponseWriter) Header() http.Header {
	return rw.headers
}

func (rw *testResponseWriter) WriteHeader(status int) {
	rw.status = status
}
