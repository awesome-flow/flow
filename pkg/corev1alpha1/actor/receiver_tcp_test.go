package actor

import (
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
	flowtest "github.com/awesome-flow/flow/pkg/util/test/corev1alpha1"
)

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
	if c.offset == l {
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

func TestHandleConn(t *testing.T) {
	nthreads := 4
	ctx, err := newContextWithConfig(map[string]interface{}{})
	if err != nil {
		t.Fatalf("failed to create context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}

	type testStruct struct {
		name    string
		status  core.MsgStatus
		sync    bool
		expresp []byte
	}

	tests := make([]testStruct, 0, len(MsgStatusToTcpResp))
	for status, resp := range MsgStatusToTcpResp {
		tests = append(tests, testStruct{
			name:    fmt.Sprintf("response with %s", sts2name(status)),
			status:  status,
			sync:    true,
			expresp: resp,
		})
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			conn := newTestConn(
				newTestAddr("tcp", "127.0.0.1:12345"),
				newTestAddr("tcp", "127.0.0.1:23456"),
			)
			params := core.Params{
				"bind": "127.0.0.1:12345",
				"sync": testCase.sync,
			}
			rcv, err := NewReceiverTCP("receiver", ctx, params)
			if err != nil {
				t.Fatalf("failed to create receiver: %s", err)
			}

			peer, err := flowtest.NewTestActor("test-actor", ctx, core.Params{})
			if err != nil {
				t.Fatalf("failed to create test actor: %s", err)
			}
			if err := rcv.Connect(nthreads, peer); err != nil {
				t.Fatalf("failed to connect test receiver: %s", err)
			}

			mailbox := make(chan *core.Message, 1)
			defer close(mailbox)

			peer.(*flowtest.TestActor).OnReceive(func(msg *core.Message) {
				mailbox <- msg
				msg.Complete(testCase.status)
				peer.(*flowtest.TestActor).Flush()
			})

			body := testutil.RandBytes(1024)
			if _, err := conn.Write(body); err != nil {
				t.Fatalf("failed to write body data to test conn: %s", err)
			}

			rcv.(*ReceiverTCP).handleConn(conn)
			if !reflect.DeepEqual(conn.buf, testCase.expresp) {
				t.Fatalf("unexpected conn buf: got: %s, want: %s", string(conn.buf), testCase.expresp)
			}

			msg := <-mailbox
			if !reflect.DeepEqual(msg.Body(), body) {
				t.Fatalf("unexpected message body: got: %s, want: %s", string(msg.Body()), string(body))
			}
		})
	}

}
