package actor

import (
	"fmt"
	"net"
	"sync"
	"time"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

const (
	UDPConnTimeout = 1 * time.Second
)

type SinkUDP struct {
	name  string
	ctx   *core.Context
	queue chan *core.Message
	addr  *net.UDPAddr
	conn  net.Conn
	lock  sync.Mutex
}

var _ core.Actor = (*SinkUDP)(nil)

func NewSinkUDP(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	bind, ok := params["bind"]
	if !ok {
		return nil, fmt.Errorf("udp sink %q is missing `bind` config", name)
	}
	addr, err := net.ResolveUDPAddr("udp", bind.(string))
	if err != nil {
		return nil, err
	}
	return &SinkUDP{
		name:  name,
		ctx:   ctx,
		queue: make(chan *core.Message),
		addr:  addr,
	}, nil
}

func (u *SinkUDP) Name() string {
	return u.name
}

func (u *SinkUDP) Start() error {
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
				u.ctx.Logger().Debug("udp sink %q is establishing a new connection to %s", u.name, u.addr)
				if err := u.connectUDP(); err != nil {
					u.ctx.Logger().Warn("udp sink %q failed to establish a new connection %s; next retry in %s ms", u.name, err, backoff)
					time.Sleep(backoff)
					backoff *= 2
					continue
				}
				u.ctx.Logger().Debug("udp sink %q has successfully established a new connection to %s", u.name, u.addr)
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
		for msg := range u.queue {
			rc = false
			if u.conn == nil {
				u.ctx.Logger().Error("udp sink %q connection is nil, dropping the message", u.name)
				msg.Complete(core.MsgStatusFailed)
				rc = true
			} else if _, err := u.conn.Write(msg.Body()); err != nil {
				msg.Complete(core.MsgStatusFailed)
				u.ctx.Logger().Error("udp sink %q failed to send a message: %s", u.name, err)
				rc = true
			} else {
				msg.Complete(core.MsgStatusDone)
			}
			if rc && len(reconnect) == 0 && len(connecting) == 0 {
				reconnect <- struct{}{}
			}
		}
		close(reconnect)
		close(connecting)
	}()

	reconnect <- struct{}{}

	return nil
}

func (u *SinkUDP) Stop() error {
	close(u.queue)
	return nil
}

func (u *SinkUDP) Connect(int, core.Receiver) error {
	return fmt.Errorf("sink %q can not connect to other receivers", u.name)
}

func (u *SinkUDP) Receive(msg *core.Message) error {
	u.queue <- msg
	return nil
}

func (u *SinkUDP) connectUDP() error {
	u.lock.Lock()
	defer u.lock.Unlock()
	u.conn = nil
	c, err := net.DialTimeout("udp", u.addr.String(), UDPConnTimeout)
	if err != nil {
		return err
	}

	u.conn = c

	return nil
}
