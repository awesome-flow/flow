package actor

import (
	"fmt"
	"net"
	"sync"
	"time"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

const (
	TCPConnTimeout = 1 * time.Second
)

type SinkTCP struct {
	name  string
	ctx   *core.Context
	queue chan *core.Message
	addr  *net.TCPAddr
	conn  net.Conn
	lock  sync.Mutex
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
		name:  name,
		ctx:   ctx,
		queue: make(chan *core.Message),
		addr:  addr,
	}, nil
}

func (t *SinkTCP) Name() string {
	return t.name
}

func (t *SinkTCP) Start() error {
	reconnect := make(chan struct{}, 1)
	connecting := make(chan struct{}, 1)

	go func() {
		for range reconnect {
			if len(connecting) != 0 {
				continue
			}
			connecting <- struct{}{}
			backoff := 50 * time.Millisecond
			for {
				t.ctx.Logger().Debug("tcp sink %q is establishing a new connection to %s", t.name, t.addr.String())
				if err := t.connectTCP(); err != nil {
					t.ctx.Logger().Warn("tcp sink %q failed to establish a tcp connection: %s; next retry in %s ms", t.name, err, backoff)
					time.Sleep(backoff)
					backoff *= 2
					continue
				}
				t.ctx.Logger().Debug("tcp sink %q has successfully established a new connection to %s", t.name, t.addr.String())
				break
			}
			if len(reconnect) > 0 {
				<-reconnect
			}
			<-connecting
		}
	}()

	go func() {
		var rc bool
		for msg := range t.queue {
			rc = false
			if t.conn == nil {
				t.ctx.Logger().Error("tcp sink %q connection is nil, dropping the message", t.name)
				msg.Complete(core.MsgStatusFailed)
				rc = true
			} else if _, err := t.conn.Write(append(msg.Body(), '\r', '\n')); err != nil {
				msg.Complete(core.MsgStatusFailed)
				t.ctx.Logger().Error("tcp sink %q failed to send a message: %s", t.name, err)
				rc = true
			} else {
				msg.Complete(core.MsgStatusDone)
			}
			if rc && len(reconnect) == 0 && len(connecting) == 0 {
				reconnect <- struct{}{}
			}
		}
	}()

	reconnect <- struct{}{}

	return nil
}

func (t *SinkTCP) Stop() error {
	close(t.queue)
	return nil
}

func (t *SinkTCP) Connect(int, core.Receiver) error {
	return fmt.Errorf("sink %q can not connect to other receivers", t.name)
}

func (t *SinkTCP) Receive(msg *core.Message) error {
	t.ctx.Logger().Debug("received a new message: %q", msg.Body())
	t.queue <- msg
	return nil
}

func (t *SinkTCP) connectTCP() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.conn = nil
	c, err := net.DialTimeout("tcp", t.addr.String(), TCPConnTimeout)
	if err != nil {
		return err
	}

	t.conn = c

	return nil
}
