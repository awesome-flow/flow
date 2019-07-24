package actor

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

func TestSinkHeadUnixConnect(t *testing.T) {
	conn := newTestConn(
		newTestAddr("unix", "/dev/null"),
		newTestAddr("unix", "/dev/null"),
	)
	connbuilder := func(addr *net.UnixAddr, timeout time.Duration) (net.Conn, error) {
		return conn, nil
	}
	addr, err := net.ResolveUnixAddr("unix", "/dev/null")
	if err != nil {
		t.Fatalf("failed to resolve unix addr: %s", err)
	}
	head, err := NewSinkHeadUnix(addr)
	if err != nil {
		t.Fatalf("failed to initialize unix sink: %s", err)
	}
	head.connbuilder = connbuilder
	if err := head.Connect(); err != nil {
		t.Fatalf("failed to connect: %s", err)
	}
	if !reflect.DeepEqual(head.conn, conn) {
		t.Fatalf("unexpected head conn: got: %+v, want: %+v", head.conn, conn)
	}
}

func TestSinkHeadUnixConnectFail(t *testing.T) {
	wanterr := fmt.Errorf("expected error")
	connbuilder := func(addr *net.UnixAddr, timeout time.Duration) (net.Conn, error) {
		return nil, wanterr
	}
	addr, err := net.ResolveUnixAddr("unix", "/dev/null")
	if err != nil {
		t.Fatalf("failed to resolve unix addr: %s", err)
	}
	head, err := NewSinkHeadUnix(addr)
	if err != nil {
		t.Fatalf("failed to initialize unix sink: %s", err)
	}
	head.connbuilder = connbuilder
	err = head.Connect()
	if !reflect.DeepEqual(err, wanterr) {
		t.Fatalf("unexpected error from head conn: got: %s, want: %s", err, wanterr)
	}
}

func TestSinkHeadUnixWrite(t *testing.T) {
	conn := newTestConn(
		newTestAddr("unix", "/dev/null"),
		newTestAddr("unix", "/dev/null"),
	)
	addr, err := net.ResolveUnixAddr("unix", "/dev/null")
	if err != nil {
		t.Fatalf("failed to resolve unix addr: %s", err)
	}
	head, err := NewSinkHeadUnix(addr)
	if err != nil {
		t.Fatalf("failed to initialize unix sink: %s", err)
	}
	head.conn = conn
	data := testutil.RandBytes(1024)
	n, err, rec := head.Write(data)
	expn := len(data) + 2
	if n != expn {
		t.Fatalf("unexpected bytes written count: got: %d, want: %d", n, expn)
	}
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if rec {
		t.Fatalf("unexpected reconnect flag set to %t", rec)
	}
	expdata := append(data, '\r', '\n')
	if !reflect.DeepEqual(conn.buf, expdata) {
		t.Fatalf("unexpected data in conn buffer: got: %q, want: %q", string(conn.buf), string(expdata))
	}
}
