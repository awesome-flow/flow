package actor

import (
	"fmt"
	"time"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type Sink struct {
	name      string
	ctx       *core.Context
	head      SinkHead
	queue     chan *core.Message
	reconnect chan chan struct{}
	done      chan struct{}
}

var _ core.Actor = (*Sink)(nil)

func NewSink(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	head, err := SinkHeadFactory(params)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sink %q: %s", name, err)
	}
	return &Sink{
		name:      name,
		ctx:       ctx,
		head:      head,
		queue:     make(chan *core.Message),
		reconnect: make(chan chan struct{}),
		done:      make(chan struct{}),
	}, nil
}

func (s *Sink) Name() string {
	return s.name
}

const (
	minbackoff = 50 * time.Millisecond
	maxbackoff = 5 * time.Second
	maxretries = 20
)

func (s *Sink) doConnectHead(notify chan struct{}) error {
	isdone := false
	go func() {
		select {
		case <-s.done:
			isdone = true
		case <-notify:
		}
	}()
	backoff := minbackoff
	retried := 0
	for !isdone {
		if err := s.head.Connect(); err != nil {
			s.ctx.Logger().Error("sink %q failed to reconnect: %s", s.name, err)
			if retried >= maxretries {
				return fmt.Errorf("gave up after %d retries", retried)
			}
			s.ctx.Logger().Trace("sink %q will try to reconnect in %dms", s.name, backoff/1000000)
			time.Sleep(backoff)
			if backoff < maxbackoff {
				backoff *= 2
			}
			retried++
			continue

		}
		close(notify)
		break
	}

	return nil
}

func (s *Sink) Start() error {

	var reqreconn = func() {
		// reconnect routine will close the
		// notify channel
		notify := make(chan struct{})
		s.reconnect <- notify
		<-notify
	}

	go func() {
		for notify := range s.reconnect {
			if err := s.doConnectHead(notify); err != nil {
				// Fatal error here: giving up and crashing
				s.ctx.Logger().Fatal("sink %q failed to reconnect: %s", s.name, err)
			}
		}
	}()

	go func() {
		for msg := range s.queue {
			if _, err, rec := s.head.Write(msg.Body()); err != nil {
				s.ctx.Logger().Error("sink %q failed to send message: %s", s.name, err)
				msg.Complete(core.MsgStatusFailed)
				if rec {
					reqreconn()
				}
				continue
			}
			msg.Complete(core.MsgStatusDone)
		}
	}()

	if err := s.head.Start(); err != nil {
		return err
	}

	reqreconn()

	return nil
}

func (s *Sink) Stop() error {
	if err := s.head.Stop(); err != nil {
		return err
	}
	close(s.queue)
	close(s.reconnect)
	close(s.done)

	return nil
}

func (s *Sink) Connect(int, core.Receiver) error {
	return fmt.Errorf("sink %q can not connect to other receivers", s.name)
}

func (s *Sink) Receive(msg *core.Message) error {
	s.queue <- msg
	return nil
}
