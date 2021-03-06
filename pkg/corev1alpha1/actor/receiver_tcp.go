package actor

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
)

const (
	ConnReadTimeout  = 50 * time.Millisecond
	ConnWriteTimeout = 50 * time.Millisecond
	MsgSendTimeout   = 100 * time.Millisecond

	DefaultBufSize = 4 * 1024
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
	bufsize  int
	addr     *net.TCPAddr
	listener net.Listener
	queue    chan *core.Message
	done     chan struct{}
	wgconn   sync.WaitGroup
	wgpeer   sync.WaitGroup
}

var _ core.Actor = (*ReceiverTCP)(nil)

func NewReceiverTCP(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	bind, ok := params["bind"]
	if !ok {
		return nil, fmt.Errorf("tcp receiver %q is missing `bind` config", name)
	}

	addr, err := net.ResolveTCPAddr("tcp", bind.(string))
	if err != nil {
		return nil, err
	}

	var silent bool
	if s, ok := params["silent"]; ok {
		if s.(string) == "true" {
			silent = true
		} else if s.(string) != "false" {
			return nil, fmt.Errorf("tcp receiver %q got an unexpected (non-bool) value for silent: %q", name, s)
		}
	}

	bufsize, ok := params["buf_size"]
	if !ok {
		bufsize = DefaultBufSize
	}

	return &ReceiverTCP{
		ctx:     ctx,
		name:    name,
		addr:    addr,
		silent:  silent,
		bufsize: bufsize.(int),
		queue:   make(chan *core.Message),
		done:    make(chan struct{}),
	}, nil
}

func (r *ReceiverTCP) Name() string {
	return r.name
}

func (r *ReceiverTCP) Start() error {
	l, err := net.Listen("tcp", r.addr.String())
	if err != nil {
		return err
	}
	r.listener = l
	nthreads, ok := r.ctx.Config().Get(types.NewKey(cfg.SystemMaxprocs))
	if !ok {
		return fmt.Errorf("failed to fetch %q config", cfg.SystemMaxprocs)
	}

	go func() {
		<-r.done
		r.ctx.Logger().Info("closing tcp listener at %s", r.addr.String())
		if err := l.Close(); err != nil {
			r.ctx.Logger().Error("failed to close tcp listener gracefuly: %s", err)
		}
	}()

	for i := 0; i < nthreads.(int); i++ {
		go func() {
			r.ctx.Logger().Info("starting tcp listener at %s", r.addr.String())
			for {
				conn, err := l.Accept()
				if err != nil {
					r.ctx.Logger().Error(err.Error())
					continue
				}
				go r.handleConn(conn)
				select {
				case <-r.done:
					break
				default:
				}
			}
		}()
	}

	return nil
}

func (r *ReceiverTCP) Stop() error {
	close(r.done)
	r.wgconn.Wait()
	close(r.queue)
	r.wgpeer.Wait()
	return nil
}

func (r *ReceiverTCP) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		r.wgpeer.Add(1)
		go func() {
			var err error
			for msg := range r.queue {
				if err = peer.Receive(msg); err != nil {
					msg.Complete(core.MsgStatusFailed)
					r.ctx.Logger().Error(err.Error())
				}
			}
			r.wgpeer.Done()
		}()
	}
	return nil
}

func (t *ReceiverTCP) Receive(*core.Message) error {
	return fmt.Errorf("tcp receiver %q can not receive internal messages", t.name)
}

// dropCR drops a terminal \r from the data.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

func ScanBin(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, []byte{'\r', '\n'}); i >= 0 {
		// We have a full newline-terminated line.
		return i + 2, dropCR(data[0:i]), nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}
	// Request more data.
	return 0, nil, nil
}

func (r *ReceiverTCP) handleConn(conn net.Conn) {
	r.ctx.Logger().Debug("new tcp connection from %s", conn.RemoteAddr())

	r.wgconn.Add(1)

	reader := bufio.NewReader(conn)
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 1024)
	scanner.Buffer(buf, r.bufsize)
	scanner.Split(ScanBin)

	isdone := false
	scanover := make(chan struct{})
	go func() {
		select {
		case <-r.done:
			isdone = true
		case <-scanover:
		}
	}()

	for !isdone && scanner.Scan() {
		msg := core.NewMessage(scanner.Bytes())
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
	close(scanover)
	if err := scanner.Err(); err != nil {
		r.ctx.Logger().Error(err.Error())
	}

	r.wgconn.Done()

	r.ctx.Logger().Debug("closing tcp connnection from %s", conn.RemoteAddr())
}
