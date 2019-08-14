package actor

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	coretest "github.com/awesome-flow/flow/pkg/corev1alpha1/test"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

func TestSinkDoConnectHead(t *testing.T) {
	ctx, err := coretest.NewContextWithConfig(map[string]interface{}{})
	if err != nil {
		t.Fatalf("failed to initialise context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	// addresses don't really matter
	conn := newTestConn(
		newTestAddr("tcp", "127.0.0.1:12345"),
		newTestAddr("tcp", "127.0.0.1:23456"),
	)
	// connection builder fails once to exercise the retry mecahnism
	failonce := 0
	builder := func(addr *net.TCPAddr, timeout time.Duration) (net.Conn, error) {
		if failonce > 0 {
			return conn, nil
		}
		failonce++
		return nil, fmt.Errorf("expected failure")
	}
	// this address would be a subject of a tcp host lookup, so keep it real
	sink, err := NewSink("sink", ctx, core.Params{
		"bind": "tcp://127.0.0.1:12345",
	})
	if err != nil {
		t.Fatalf("failed to initialize sink: %s", err)
	}
	sink.(*Sink).head.(*SinkHeadTCP).connbuilder = builder
	notify := make(chan struct{})
	if wantnil := sink.(*Sink).head.(*SinkHeadTCP).conn; wantnil != nil {
		t.Fatalf("unexpected connection value: got: %+v, want: nil", wantnil)

	}
	if err := sink.(*Sink).doConnectHead(notify); err != nil {
		t.Fatalf("failed to connect sink head: %s", err)
	}
	<-notify
	gotconn := sink.(*Sink).head.(*SinkHeadTCP).conn
	if !reflect.DeepEqual(conn, gotconn) {
		t.Fatalf("unexpected connection object: got: %+v, want: %+v", gotconn, conn)
	}
}

func TestSinkStartStop(t *testing.T) {
	ctx, err := coretest.NewContextWithConfig(map[string]interface{}{
		cfg.SystemMaxprocs: 4,
	})
	if err != nil {
		t.Fatalf("failed to initialise context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	// addresses don't really matter
	conn := newTestConn(
		newTestAddr("tcp", "127.0.0.1:12345"),
		newTestAddr("tcp", "127.0.0.1:23456"),
	)
	// connection builder fails once to exercise the retry mecahnism
	failonce := 0
	builder := func(addr *net.TCPAddr, timeout time.Duration) (net.Conn, error) {
		if failonce > 0 {
			return conn, nil
		}
		failonce++
		return nil, fmt.Errorf("expected failure")
	}
	// this address would be a subject of a tcp host lookup, so keep it real
	sink, err := NewSink("sink", ctx, core.Params{
		"bind": "tcp://127.0.0.1:12345",
	})
	if err != nil {
		t.Fatalf("failed to initialize sink: %s", err)
	}
	sink.(*Sink).head.(*SinkHeadTCP).connbuilder = builder
	if err := sink.Start(); err != nil {
		t.Fatalf("failed to start sink: %s", err)
	}
	if err := sink.Stop(); err != nil {
		t.Fatalf("failed to stop sink: %s", err)
	}
}

func TestSinkReceive(t *testing.T) {
	ctx, err := coretest.NewContextWithConfig(map[string]interface{}{
		cfg.SystemMaxprocs: 4,
	})
	if err != nil {
		t.Fatalf("failed to initialise context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	// addresses don't really matter
	conn := newTestConn(
		newTestAddr("tcp", "127.0.0.1:12345"),
		newTestAddr("tcp", "127.0.0.1:23456"),
	)
	builder := func(addr *net.TCPAddr, timeout time.Duration) (net.Conn, error) {
		return conn, nil
	}
	// this address would be a subject of a tcp host lookup, so keep it real
	sink, err := NewSink("sink", ctx, core.Params{
		"bind": "tcp://127.0.0.1:12345",
	})
	if err != nil {
		t.Fatalf("failed to initialize sink: %s", err)
	}
	sink.(*Sink).head.(*SinkHeadTCP).connbuilder = builder
	if err := sink.Start(); err != nil {
		t.Fatalf("failed to start sink: %s", err)
	}
	data := testutil.RandBytes(1024)
	msg := core.NewMessage(data)
	if err := sink.Receive(msg); err != nil {
		t.Fatalf("failed to send message: %s", err)
	}
	s := msg.Await()
	if s != core.MsgStatusDone {
		t.Fatalf("unexpected message send status: got: %d, want: %d", s, core.MsgStatusDone)
	}
	wantdata := append(data, []byte("\r\n")...)
	gotdata := conn.buf
	if !reflect.DeepEqual(gotdata, wantdata) {
		t.Fatalf("unexpected buf contents: got: %q, want: %q", gotdata, wantdata)
	}
}
