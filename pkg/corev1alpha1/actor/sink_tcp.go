package actor

import (
	"fmt"
	"net"
	"time"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

const (
	TCPConnTimeout = 1 * time.Second
)

type TCPConnBuilder func(addr *net.TCPAddr, timeout time.Duration) (net.Conn, error)

var defaultTCPConnBuilder = func(addr *net.TCPAddr, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr.String(), timeout)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

type SinkTCP struct {
	name      string
	ctx       *core.Context
	addr      *net.TCPAddr
	queue     chan *core.Message
	reconnect chan chan struct{}
	done      chan struct{}
	conn      net.Conn
	builder   TCPConnBuilder
}

var _ core.Actor = (*SinkTCP)(nil)

func NewSinkTCP(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	bind, ok := params["bind"]
	if !ok {
		return nil, fmt.Errorf("tcp sink %q is missing `bind` config", name)
	}
	addr, err := net.ResolveTCPAddr("tcp", bind.(string))
	if err != nil {
		return nil, err
	}
	return &SinkTCP{
		name:      name,
		ctx:       ctx,
		addr:      addr,
		queue:     make(chan *core.Message),
		reconnect: make(chan chan struct{}),
		builder:   defaultTCPConnBuilder,
		done:      make(chan struct{}),
	}, nil
}

func (t *SinkTCP) Name() string {
	return t.name
}

var maxbackoff time.Duration = 10 * time.Second

func (t *SinkTCP) doConnectTCP(notify chan struct{}) error {
	backoff := 50 * time.Millisecond

	isdone := false
	go func() {
		select {
		case <-t.done:
			isdone = true
		case <-notify:
		}
	}()

	for !isdone {
		t.ctx.Logger().Debug("tcp sink %q is establishing a new connection to %s", t.name, t.addr)
		conn, err := t.builder(t.addr, TCPConnTimeout)
		if err != nil {
			t.conn = nil
			t.ctx.Logger().Warn("tcp sink %q failed to establish a new connection: %s; next retry in %s ms", t.name, err, backoff)
			time.Sleep(backoff)
			if backoff < maxbackoff {
				backoff *= 2
			}
			continue
		}
		t.conn = conn
		t.ctx.Logger().Debug("tcp sink %q has successfully established a new connection to %s", t.name, t.addr)
		if notify != nil {
			close(notify)
		}

		break
	}

	return nil
}

func (t *SinkTCP) doSend(msg *core.Message) error {
	if t.conn == nil {
		msg.Complete(core.MsgStatusFailed)
		return fmt.Errorf("conn is nil")
	}

	l := len(msg.Body())
	data := make([]byte, l+2)
	copy(data, msg.Body())
	copy(data[l:], []byte("\r\n"))

	if _, err := t.conn.Write(data); err != nil {
		msg.Complete(core.MsgStatusFailed)
		return err
	}
	msg.Complete(core.MsgStatusDone)

	return nil
}

func (t *SinkTCP) Start() error {
	go func() {
		for notify := range t.reconnect {
			if err := t.doConnectTCP(notify); err != nil {
				t.ctx.Logger().Error("tcp sink %q failed to connect: %s", t.name, err)
			}
		}
	}()

	go func() {
		for msg := range t.queue {
			if err := t.doSend(msg); err != nil {
				t.ctx.Logger().Error("tcp sink %q failed to send message: %q", t.name, err)
				if t.conn == nil {
					notify := make(chan struct{})
					t.reconnect <- notify
					<-notify
				}
			}
		}
	}()

	return t.doConnectTCP(nil)
}

func (t *SinkTCP) Stop() error {
	close(t.queue)
	<-t.done

	return nil
}

func (t *SinkTCP) Connect(int, core.Receiver) error {
	return fmt.Errorf("sink %q can not connect to other receivers", t.name)
}

func (t *SinkTCP) Receive(msg *core.Message) error {
	t.queue <- msg
	return nil
}
