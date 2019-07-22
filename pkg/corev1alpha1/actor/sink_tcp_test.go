package actor

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

func TestDoConnectTCP(t *testing.T) {
	ctx, err := newContextWithConfig(map[string]interface{}{})
	if err != nil {
		t.Fatalf("failed to create context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	bindaddr := "127.0.0.1:12345"
	sink, err := NewSinkTCP("tcp-sink", ctx, core.Params{
		"bind": bindaddr,
	})
	if err != nil {
		t.Fatalf("failed to create tcp sink: %s", err)
	}
	conn := newTestConn(
		newTestAddr("tcp", "127.0.0.1:1234"),
		newTestAddr("tcp", "127.0.0.1:2345"),
	)
	var lock sync.Mutex
	var cnt int
	sink.(*SinkTCP).builder = func(addr *net.TCPAddr, timeout time.Duration) (net.Conn, error) {
		lock.Lock()
		defer lock.Unlock()
		cnt++
		if cnt > 2 {
			conn.localaddr = addr
			return conn, nil
		}
		return nil, fmt.Errorf("expected failure")
	}
	notify := make(chan struct{})
	go func() {
		if err := sink.(*SinkTCP).doConnectTCP(notify); err != nil {
			t.Fatalf("failed to call doConnectTCP: %s", err)
		}
	}()
	<-notify
	if sink.(*SinkTCP).conn == nil {
		t.Fatalf("tcp sink conn is nil")
	}
	if conn.LocalAddr().String() != bindaddr {
		t.Fatalf("unexpected conn local addr: got: %s, want: %s", conn.LocalAddr().String(), bindaddr)
	}
}

func TestDoSend(t *testing.T) {
	ctx, err := newContextWithConfig(map[string]interface{}{})
	if err != nil {
		t.Fatalf("failed to create context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	bindaddr := "127.0.0.1:12345"
	sink, err := NewSinkTCP("tcp-sink", ctx, core.Params{
		"bind": bindaddr,
	})
	conn := newTestConn(
		newTestAddr("tcp", "127.0.0.1:12345"),
		newTestAddr("tcp", "127.0.0.1:23456"),
	)
	sink.(*SinkTCP).builder = func(*net.TCPAddr, time.Duration) (net.Conn, error) {
		return conn, nil
	}
	msg := core.NewMessage(testutil.RandBytes(1024))
	if err := sink.(*SinkTCP).doConnectTCP(nil); err != nil {
		t.Fatalf("tcp sink failed to connect: %s", err)
	}
	go func() {
		if err := sink.(*SinkTCP).doSend(msg); err != nil {
			t.Fatalf("tcp sink failed to send message: %s", err)
		}
	}()
	s := msg.Await()
	if s != core.MsgStatusDone {
		t.Fatalf("unexpected message status: got: %d, want: %d", s, core.MsgStatusDone)
	}
	var buf bytes.Buffer
	buf.Write(msg.Body())
	buf.WriteString("\r\n")
	if !reflect.DeepEqual(conn.buf, buf.Bytes()) {
		t.Fatalf("unexpected conn buf content: got: %s, want: %s", string(conn.buf), string(buf.Bytes()))
	}
}
