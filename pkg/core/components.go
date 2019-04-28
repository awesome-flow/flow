package core

import (
	"math/rand"
	"runtime"
	"sync"

	"github.com/awesome-flow/flow/pkg/config"
	"github.com/awesome-flow/flow/pkg/devenv"
)

type RoutingFunc func(*Message) (string, error)

const (
	BufChanSize = 65535
)

// Context is there to provide a current state of the link.
// Anything but linkContent should remain stateless.
type Context struct {
	msgCh   []chan *Message
	cmdIn   chan *Cmd
	cmdOut  chan *Cmd
	storage *sync.Map
	thrdns  int
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func NewContext() *Context {
	th, _ := config.GetOrDefault("global.system.maxprocs", runtime.GOMAXPROCS(-1))
	threadiness := maxInt(th.(int), 1)
	msgChannels := make([]chan *Message, 0, threadiness)

	for i := 0; i < threadiness; i++ {
		msgChannels = append(msgChannels, make(chan *Message, BufChanSize))
	}
	return &Context{
		msgCh:   msgChannels,
		cmdIn:   make(chan *Cmd),
		cmdOut:  make(chan *Cmd),
		storage: &sync.Map{},
		thrdns:  threadiness,
	}
}

func NewContextUnsafe(msgCh []chan *Message,
	cmdIn chan *Cmd, cmdOut chan *Cmd, storage *sync.Map) *Context {
	return &Context{
		msgCh:   msgCh,
		cmdIn:   cmdIn,
		cmdOut:  cmdOut,
		storage: storage,
	}
}

func (c *Context) GetMsgCh() []chan *Message {
	return c.msgCh
}

func (c *Context) GetCmdIn() chan *Cmd {
	return c.cmdIn
}

func (c *Context) GetCmdOut() chan *Cmd {
	return c.cmdOut
}

func (c *Context) GetVal(key string) (interface{}, bool) {
	val, ok := c.storage.Load(key)
	return val, ok
}

func (c *Context) SetVal(key string, value interface{}) {
	c.storage.Store(key, value)
}

type Link interface {
	String() string
	Recv(*Message) error
	Send(*Message) error
	ConnectTo(Link) error
	LinkTo([]Link) error
	RouteTo(map[string]Link) error
	ExecCmd(*Cmd) error
	GetContext() *Context
	DevEnv(*devenv.Context) ([]devenv.Fragment, error)
}

type Connector struct {
	onsetup    func() error
	onteardown func() error

	startonce sync.Once
	stoponce  sync.Once

	starterr error
	stoperr  error

	context *Context
}

func NewConnector() *Connector {
	return NewConnectorWithContext(NewContext())
}

func NewConnectorWithContext(context *Context) *Connector {
	connector := &Connector{context: context}
	connector.onsetup = connector.SetUp
	connector.onteardown = connector.TearDown

	return connector
}

func (cn *Connector) OnSetUp(onsetup func() error) {
	cn.onsetup = onsetup
}

func (cn *Connector) OnTearDown(onteardown func() error) {
	cn.onteardown = onteardown
}

func (cn *Connector) Start() error {
	cn.startonce.Do(func() {
		cn.starterr = cn.onsetup()
	})
	return cn.starterr
}

func (cn *Connector) SetUp() error {
	return nil
}

func (cn *Connector) Stop() error {
	cn.stoponce.Do(func() {
		cn.stoperr = cn.onteardown()
	})
	return cn.stoperr
}

func (cn *Connector) TearDown() error {
	return nil
}

func (cn *Connector) Reset() error {
	cn.stoponce = sync.Once{}
	if err := cn.Stop(); err != nil {
		return err
	}
	cn.startonce = sync.Once{}
	if err := cn.Start(); err != nil {
		return err
	}
	return nil
}

func (cn *Connector) Recv(msg *Message) error {
	return cn.Send(msg)
}

func (cn *Connector) Send(msg *Message) error {
	rnd := rand.Intn(cn.context.thrdns)
	cn.context.msgCh[rnd] <- msg
	return nil
}

func (cn *Connector) ExecCmd(cmd *Cmd) error {
	switch cmd.Code {
	case CmdCodeStart:
		return cn.Start()
	case CmdCodeStop:
		return cn.Stop()
	default:
		return nil
	}
}

func (cn *Connector) ConnectTo(l Link) error {
	for i := 0; i < len(cn.context.msgCh); i++ {
		go func(ch chan *Message) {
			for msg := range ch {
				l.Recv(msg)
			}
		}(cn.context.msgCh[i])
	}
	return nil
}

func (cn *Connector) LinkTo([]Link) error {
	panic("This package does not support LinkTo()")
}

func (cn *Connector) RouteTo(map[string]Link) error {
	panic("This package does not support RouteTo()")
}

func (cn *Connector) GetMsgCh() []chan *Message {
	return cn.context.msgCh
}

func (cn *Connector) String() string {
	return "A connector"
}

func (cn *Connector) GetContext() *Context {
	return cn.context
}

func (cn *Connector) DevEnv(ctx *devenv.Context) ([]devenv.Fragment, error) {
	return nil, nil
}
