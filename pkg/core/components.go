package core

import (
	"math/rand"
	"runtime"
	"sync"

	"github.com/awesome-flow/flow/pkg/devenv"
)

// RoutingFunc is a generic routing routine signature.
type RoutingFunc func(*Message) (string, error)

const (
	// BufChanSize is the default size for buffered message channels.
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

// NewContext creates a new link context.
func NewContext() *Context {
	// By the moment it's fetched the pipeline should already have applied
	// the system settings.
	threadiness := runtime.GOMAXPROCS(-1)

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

// NewContextUnsafe is an alternative constructor for Context accepting
// user-defined channels.
func NewContextUnsafe(msgCh []chan *Message,
	cmdIn chan *Cmd, cmdOut chan *Cmd, storage *sync.Map) *Context {
	return &Context{
		msgCh:   msgCh,
		cmdIn:   cmdIn,
		cmdOut:  cmdOut,
		storage: storage,
	}
}

// MsgCh returns Context message channel.
func (c *Context) MsgCh() []chan *Message {
	return c.msgCh
}

// CmdIn returns Context command propagation channel.
// TODO: review this interface
func (c *Context) CmdIn() chan *Cmd {
	return c.cmdIn
}

// CmdOut returns Context command return channel.
// TODO: review this interface
func (c *Context) CmdOut() chan *Cmd {
	return c.cmdOut
}

// Val returns a value stored in the Context storage. A bool flag indicates
// lookup status.
// TODO: review this interface
func (c *Context) Val(key string) (interface{}, bool) {
	val, ok := c.storage.Load(key)
	return val, ok
}

// SetVal sets a value in the Context storage under the provided key.
// TODO: review this interface
func (c *Context) SetVal(key string, value interface{}) {
	c.storage.Store(key, value)
}

// Link is the primary interface for Flow components. A Link implementation
// defines the logic of message handling, the strategy of connectiow with other
// links and reaction to the commands.
type Link interface {
	// String satisfies Stringer interface.
	String() string
	// Recv receives incomming messages and returns an error if the message
	// couldn't be accepted.
	Recv(*Message) error
	// Send forwards the message down the pipe from the Link instance.
	Send(*Message) error
	// ConnectTo creates a 1-to-1 connection between 2 links.
	ConnectTo(Link) error
	// LinkTo creates a 1-to-many connection from the current Link to the
	// Links in the list.
	LinkTo([]Link) error
	// RouteTo creates an associative 1-to-many connction from the current
	// Link to the Links in the map.
	RouteTo(map[string]Link) error
	// ExecCmd accepts a command and executes it on the Link. Returns an
	// error if the exectution fails.
	ExecCmd(*Cmd) error
	// Context returns the current Link context.
	Context() *Context
	// DevEnv returns a list of devenv Fragments (if any defined) so every
	// Link type can define the logic of setting up dev environment helpers.
	// TODO: review this interface.
	DevEnv(*devenv.Context) ([]devenv.Fragment, error)
}

// Connctor is the glue layer between Links. It encorporates the logic of
// message pipelining, initialisation and teardown processes.
// Connector implements Link interface and is expected to serve as an embedded
// basis for user-defined links.
type Connector struct {
	onsetup    func() error
	onteardown func() error

	startonce sync.Once
	stoponce  sync.Once

	starterr error
	stoperr  error

	context *Context
}

var _ Link = (*Connector)(nil)

// NewConnector is the default constructor for Connector. A new Context is being
// created immediately.
func NewConnector() *Connector {
	return NewConnectorWithContext(NewContext())
}

// NewConnectorWithContext is an alternative constructor for Context with an
// extra argument: a user-provided Context.
func NewConnectorWithContext(context *Context) *Connector {
	connector := &Connector{context: context}
	connector.onsetup = connector.SetUp
	connector.onteardown = connector.TearDown

	return connector
}

// OnSetUp registers a callback that's going to be called on the context Start
// stage.
func (cn *Connector) OnSetUp(onsetup func() error) {
	cn.onsetup = onsetup
}

// OnTearDown registers a callbac that's going to be called on the context Stop
// stage.
func (cn *Connector) OnTearDown(onteardown func() error) {
	cn.onteardown = onteardown
}

// Start executes the Connector start logic.
func (cn *Connector) Start() error {
	cn.startonce.Do(func() {
		cn.starterr = cn.onsetup()
	})
	return cn.starterr
}

// SetUp is the default Start reactor of the Connector.
func (cn *Connector) SetUp() error {
	return nil
}

// Stop executes the Connector stop logic.
func (cn *Connector) Stop() error {
	cn.stoponce.Do(func() {
		cn.stoperr = cn.onteardown()
	})
	return cn.stoperr
}

// TearDown is the default TearDown reactor of the Connector.
func (cn *Connector) TearDown() error {
	return nil
}

// Reset clears up the state of the Connector: after this operation it can be
// started and stopped again.
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

// Recv injects the Message into the Connector pipeine. Returns an error if the
// operation can't be completed.
func (cn *Connector) Recv(msg *Message) error {
	return cn.Send(msg)
}

// Send encorporates the Message downstream submission logic: interaction with
// the connected links and ingestion into their pipelines.
func (cn *Connector) Send(msg *Message) error {
	rnd := rand.Intn(cn.context.thrdns)
	cn.context.msgCh[rnd] <- msg
	return nil
}

// ExecCmd is an interface for executing a Command on the Connector. Returns an
// error if the execution failed.
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

// ConnectTo creates a new 1-to-1 connection between the Connector and the
// argument Link.
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

// LinkTo creates a new 1-to-many connection between the Connector and the
// argument Lnks list. Expected to be redefined by specific Link
// implementations.
func (cn *Connector) LinkTo([]Link) error {
	panic("This package does not support LinkTo()")
}

// RouteTo creates a new 1-to-many associative connection between the Connector
// and the arguments Links map. Expected to be redefined by specific Link
// implementations.
func (cn *Connector) RouteTo(map[string]Link) error {
	panic("This package does not support RouteTo()")
}

// MsgCh returns the Connector message channel.
func (cn *Connector) MsgCh() []chan *Message {
	return cn.context.msgCh
}

// String satisfies Stringer interface. Expected to be redefined in specific
// Link implementations.
func (cn *Connector) String() string {
	return "A connector"
}

// Context returns the current context of Connector.
func (cn *Connector) Context() *Context {
	return cn.context
}

// DevEnv returns the dev env Fragments. None for the plain Connector. Expected
// to be redefined in specific Link implementations.
func (cn *Connector) DevEnv(ctx *devenv.Context) ([]devenv.Fragment, error) {
	return nil, nil
}
