package core

import (
	"sync"
)

type Params map[string]interface{}

type RoutingFunc func(*Message) (string, error)

// LinkContent is there to provide a current state of the link.
// Anything but linkContent should remain stateless.
type LinkContext struct {
	msgCh  chan *Message
	cmdIn  chan *Cmd
	cmdOut chan *Cmd
	state  *sync.Map
}

func (lc *LinkContext) GetMsgCh() chan *Message {
	return lc.msgCh
}

func (lc *LinkContext) GetCmdIn() chan *Cmd {
	return lc.cmdIn
}

func (lc *LinkContext) GetCmdOut() chan *Cmd {
	return lc.cmdOut
}

func (lc *LinkContext) GetVal(key string) (interface{}, bool) {
	val, ok := lc.state.Load(key)
	return val, ok
}

func (lc *LinkContext) SetVal(key string, value interface{}) {
	lc.state.Store(key, value)
}

type Link interface {
	String() string
	Recv(*Message) error
	Send(*Message) error
	ConnectTo(Link) error
	LinkTo([]Link) error
	RouteTo(map[string]Link) error
	ExecCmd(*Cmd) error
	GetContext() *LinkContext
}

type Connector struct {
	context *LinkContext
	msgCh   chan *Message
	cmdIn   chan *Cmd
	cmdOut  chan *Cmd
}

func NewConnector() *Connector {
	return &Connector{
		msgCh:  make(chan *Message),
		cmdIn:  make(chan *Cmd),
		cmdOut: make(chan *Cmd),
	}
}

func (cn *Connector) Recv(msg *Message) error {
	return cn.Send(msg)
}

func (cn *Connector) Send(msg *Message) error {
	cn.msgCh <- msg
	return nil
}

func (cn *Connector) ExecCmd(cmd *Cmd) error {
	return nil
}

func (cn *Connector) ConnectTo(l Link) error {
	go func() {
		for msg := range cn.msgCh {
			l.Recv(msg)
		}
	}()
	return nil
}

func (cn *Connector) LinkTo([]Link) error {
	panic("This package does not support LinkTo()")
}

func (cn *Connector) RouteTo(map[string]Link) error {
	panic("This package does not support RouteTo()")
}

func (cn *Connector) GetMsgCh() chan *Message {
	return cn.msgCh
}

func (cn *Connector) String() string {
	return "A connector"
}
