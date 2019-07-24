package actor

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

func TestSinkHeadUDPConnect(t *testing.T) {
	conn := newTestConn(
		newTestAddr("udp", "127.0.0.1:12345"),
		newTestAddr("udp", "127.0.0.1:23456"),
	)
	connbuilder := func(addr *net.UDPAddr, timeout time.Duration) (net.Conn, error) {
		return conn, nil
	}
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}
	head, err := NewSinkHeadUDP(addr)
	if err != nil {
		t.Fatalf("failed to initialize udp sink: %s", err)
	}
	head.connbuilder = connbuilder
	if err := head.Connect(); err != nil {
		t.Fatalf("failed to connect: %s", err)
	}
	if !reflect.DeepEqual(head.conn, conn) {
		t.Fatalf("unexpected head conn: got: %+v, want: %+v", head.conn, conn)
	}
}

func TestSinkHeadUDPConnectFail(t *testing.T) {
	wanterr := fmt.Errorf("expected error")
	connbuilder := func(addr *net.UDPAddr, timeout time.Duration) (net.Conn, error) {
		return nil, wanterr
	}
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}
	head, err := NewSinkHeadUDP(addr)
	if err != nil {
		t.Fatalf("failed to initialize udp sink: %s", err)
	}
	head.connbuilder = connbuilder
	err = head.Connect()
	if !reflect.DeepEqual(err, wanterr) {
		t.Fatalf("unexpected error from head conn: got: %s, want: %s", err, wanterr)
	}
}

func TestSinkHeadUDPWrite(t *testing.T) {
	conn := newTestConn(
		newTestAddr("udp", "127.0.0.1:12345"),
		newTestAddr("udp", "127.0.0.1:23456"),
	)
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}
	head, err := NewSinkHeadUDP(addr)
	if err != nil {
		t.Fatalf("failed to initialize udp sink: %s", err)
	}
	head.conn = conn
	data := testutil.RandBytes(1024)
	n, err, rec := head.Write(data)
	expn := len(data)
	if n != expn {
		t.Fatalf("unexpected bytes written count: got: %d, want: %d", n, expn)
	}
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if rec {
		t.Fatalf("unexpected reconnect flag set to %t", rec)
	}
	if !reflect.DeepEqual(conn.buf, data) {
		t.Fatalf("unexpected data in conn buffer: got: %q, want: %q", string(conn.buf), string(data))
	}
}
