package actor

import (
	"bufio"
	"fmt"
	"net"
	"os"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

const (
	FlowUnixSock = "/tmp/flow.sock"
)

type ReceiverUnix struct {
	name     string
	ctx      *core.Context
	queue    chan *core.Message
	addr     *net.UnixAddr
	listener *net.UnixListener
	done     chan struct{}
}

var _ core.Actor = (*ReceiverUnix)(nil)

func NewReceiverUnix(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	bind, ok := params["bind"]
	if !ok {
		bind = FlowUnixSock
	}
	addr, err := net.ResolveUnixAddr("unix", bind.(string))
	if err != nil {
		return nil, err
	}

	return &ReceiverUnix{
		name:  name,
		ctx:   ctx,
		queue: make(chan *core.Message),
		addr:  addr,
		done:  make(chan struct{}),
	}, nil
}

func (u *ReceiverUnix) Name() string {
	return u.name
}

func (u *ReceiverUnix) Start() error {
	stat, err := os.Stat(u.addr.String())
	if err == nil {
		if stat.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("file %s already exists and it's not a unix socket: can not rebind", u.addr)
		}
		u.ctx.Logger().Warn("file %s already exists, rebinding", u.addr)
		if err := os.Remove(u.addr.String()); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	l, err := net.ListenUnix("unix", u.addr)
	if err != nil {
		return err
	}
	u.listener = l

	isdone := false
	go func() {
		<-u.done
		isdone = true
	}()

	go func() {
		u.ctx.Logger().Info("starting unix listener at %s", u.addr)
		for !isdone {
			c, err := u.listener.AcceptUnix()
			if err != nil {
				u.ctx.Logger().Error(err.Error())
				continue
			}
			go u.handleConn(c)
		}

		u.listener.Close()
	}()

	return nil
}

func (u *ReceiverUnix) Stop() error {
	close(u.done)
	close(u.queue)

	return os.Remove(u.addr.String())
}

func (u *ReceiverUnix) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		go func() {
			for msg := range u.queue {
				if err := peer.Receive(msg); err != nil {
					u.ctx.Logger().Error(err.Error())
				}
			}
		}()
	}

	return nil
}

func (u *ReceiverUnix) Receive(msg *core.Message) error {
	return fmt.Errorf("unix receiver %q can not receive internal messages", u.name)
}

func (u *ReceiverUnix) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		msg := core.NewMessage(scanner.Bytes())
		u.queue <- msg
	}

	if err := scanner.Err(); err != nil {
		u.ctx.Logger().Error(err.Error())
	}
}
