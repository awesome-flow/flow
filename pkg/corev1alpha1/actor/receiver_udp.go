package actor

import (
	"bufio"
	"fmt"
	"net"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
)

type ReceiverUDP struct {
	name  string
	ctx   *core.Context
	addr  *net.UDPAddr
	conn  *net.UDPConn
	queue chan *core.Message
	done  chan struct{}
}

var _ core.Actor = (*ReceiverUDP)(nil)

func NewReceiverUDP(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	bind, ok := params["bind"]
	if !ok {
		return nil, fmt.Errorf("udp receiver is missing `bind` config")
	}
	addr, err := net.ResolveUDPAddr("udp", bind.(string))
	if err != nil {
		return nil, err
	}

	return &ReceiverUDP{
		name:  name,
		ctx:   ctx,
		addr:  addr,
		queue: make(chan *core.Message),
		done:  make(chan struct{}),
	}, nil
}

func (r *ReceiverUDP) Name() string {
	return r.name
}

func (r *ReceiverUDP) handleConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		msg := core.NewMessage(scanner.Bytes())
		r.queue <- msg
	}

	if err := scanner.Err(); err != nil {
		r.ctx.Logger().Error(err.Error())
	}
}

func (r *ReceiverUDP) Start() error {
	r.ctx.Logger().Info("starting udp listener at %s", r.addr)
	conn, err := net.ListenUDP("udp", r.addr)
	if err != nil {
		return err
	}
	r.conn = conn

	isdone := false
	go func() {
		<-r.done
		isdone = true
	}()

	nthreads, ok := r.ctx.Config().Get(types.NewKey("system.maxprocs"))
	if !ok {
		nthreads = 1
	}

	for i := 0; i < nthreads.(int); i++ {
		go func() {
			for !isdone {
				r.handleConn(conn)
			}

			r.conn.Close()
		}()
	}

	return nil
}

func (r *ReceiverUDP) Stop() error {
	close(r.done)
	close(r.queue)

	return nil
}

func (r *ReceiverUDP) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		go func() {
			for msg := range r.queue {
				if err := peer.Receive(msg); err != nil {
					r.ctx.Logger().Error(err.Error())
				}
			}
		}()
	}
	return nil
}

func (u *ReceiverUDP) Receive(*core.Message) error {
	return fmt.Errorf("udp receiver %q can not receive internal messages", u.name)
}
