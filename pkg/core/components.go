package core

type Params map[string]interface{}

type RoutingFunc func(*Message) (string, error)

type Link interface {
	String() string
	Recv(*Message) error
	Send(*Message) error
	ConnectTo(Link) error
	LinkTo([]Link) error
	RouteTo(map[string]Link) error
	ExecCmd(*Cmd) error
}

type Connector struct {
	msgCh  chan *Message
	cmdIn  chan *Cmd
	cmdOut chan *Cmd
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
