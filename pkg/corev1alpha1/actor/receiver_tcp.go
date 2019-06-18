package actor

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

const (
	ConnReadTimeout  = 50 * time.Millisecond
	ConnWriteTimeout = 50 * time.Millisecond
	MsgSendTimeout   = 100 * time.Millisecond
)

var (
	TcpRespFail = []byte("FAILED\r\n")
	TcpRespInvd = []byte("INVALID\r\n")
	TcpRespPsnt = []byte("PARTSENT\r\n")
	TcpRespOk   = []byte("OK\r\n")
	TcpRespTime = []byte("TIMEOUT\r\n")
	TcpRespThrt = []byte("THROTTLED\r\n")
	TcpRespUnrt = []byte("UNROUTABLE\r\n")
)

var MsgStatusToTcpResp = map[core.MsgStatus][]byte{
	core.MsgStatusDone:        TcpRespOk,
	core.MsgStatusPartialSend: TcpRespPsnt,
	core.MsgStatusInvalid:     TcpRespInvd,
	core.MsgStatusFailed:      TcpRespFail,
	core.MsgStatusTimedOut:    TcpRespTime,
	core.MsgStatusUnroutable:  TcpRespUnrt,
	core.MsgStatusThrottled:   TcpRespThrt,
}

type ReceiverTCP struct {
	name     string
	ctx      *core.Context
	silent   bool
	bind     string
	listener net.Listener
	queue    chan *core.Message
	done     chan struct{}
}

var _ core.Actor = (*ReceiverTCP)(nil)

func NewReceiverTCP(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	r := &ReceiverTCP{
		ctx:   ctx,
		name:  name,
		queue: make(chan *core.Message),
		done:  make(chan struct{}),
	}

	bind, ok := params["bind"]
	if !ok {
		return nil, fmt.Errorf("tcp receiver is missing `bind` config")
	}
	r.bind = bind.(string)

	if silent, ok := params["silent"]; ok {
		if silent.(string) == "true" {
			r.silent = true
		} else if silent.(string) != "false" {
			return nil, fmt.Errorf("unknown value for silent: %q", silent)
		}
	}
	return r, nil
}

func (r *ReceiverTCP) Name() string {
	return r.name
}

func (r *ReceiverTCP) Start() error {
	l, err := net.Listen("tcp", r.bind)
	if err != nil {
		return err
	}
	r.listener = l

	isdone := false
	go func() {
		<-r.done
		isdone = true
	}()

	go func() {
		r.ctx.Logger().Info("starting tcp listener at %s", r.bind)
		for !isdone {
			conn, err := l.Accept()
			if err != nil {
				r.ctx.Logger().Error(err.Error())
			}
			go r.handleConn(conn)
		}
		r.ctx.Logger().Info("closing tcp listener at %s", r.bind)
		l.Close()
	}()

	return nil
}

func (r *ReceiverTCP) handleConn(conn net.Conn) {
	r.ctx.Logger().Debug("new tcp connection from %s", conn.RemoteAddr())
	defer conn.Close()
	reader := bufio.NewReader(conn)
	scanner := bufio.NewScanner(reader)

	//conn.SetReadDeadline(time.Now().Add(ConnReadTimeout))
	for scanner.Scan() {
		msg := core.NewMessage(scanner.Bytes())
		r.ctx.Logger().Trace(string(scanner.Bytes()))
		r.queue <- msg

		if r.silent {
			continue
		}

		var status core.MsgStatus

		select {
		case s := <-msg.AwaitChan():
			status = s
		case <-time.After(MsgSendTimeout):
			status = core.MsgStatusTimedOut
		}

		reply := MsgStatusToTcpResp[status]
		conn.SetWriteDeadline(time.Now().Add(ConnWriteTimeout))
		if _, err := conn.Write(reply); err != nil {
			r.ctx.Logger().Error(err.Error())
		}
	}
	if err := scanner.Err(); err != nil {
		r.ctx.Logger().Error(err.Error())
	}

	r.ctx.Logger().Debug("closing tcp connnection from %s", conn.RemoteAddr())
}

func (r *ReceiverTCP) Stop() error {
	close(r.done)
	close(r.queue)
	return nil
}

func (r *ReceiverTCP) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		go func() {
			var err error
			for msg := range r.queue {
				if err = peer.Receive(msg); err != nil {
					if err == io.EOF {
						return
					}
					r.ctx.Logger().Error(err.Error())
				}
			}
		}()
	}
	return nil
}

func (*ReceiverTCP) Receive(*core.Message) error {
	panic("this component can not receive messages")
}
