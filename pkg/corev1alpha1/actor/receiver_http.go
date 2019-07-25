package actor

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

const (
	ShutdownTimeout    = 5 * time.Second
	HttpMsgSendTimeout = 50 * time.Millisecond
)

type codetext struct {
	code int
	text []byte
}

var MsgStatusToHttpResp = map[core.MsgStatus]codetext{
	core.MsgStatusDone:        {http.StatusOK, []byte("OK")},
	core.MsgStatusPartialSend: {http.StatusConflict, []byte("Partial send")},
	core.MsgStatusInvalid:     {http.StatusBadRequest, []byte("Invalid message")},
	core.MsgStatusFailed:      {http.StatusInternalServerError, []byte("Failed to send")},
	core.MsgStatusTimedOut:    {http.StatusGatewayTimeout, []byte("Timed out to send message")},
	core.MsgStatusUnroutable:  {http.StatusNotAcceptable, []byte("Unknown destination")},
	core.MsgStatusThrottled:   {http.StatusTooManyRequests, []byte("Message throttled")},
}

type ReceiverHTTP struct {
	name    string
	bind    string
	ctx     *core.Context
	queue   chan *core.Message
	httpsrv *http.Server
	done    chan struct{}
}

var _ core.Actor = (*ReceiverHTTP)(nil)

func NewReceiverHTTP(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	bind, ok := params["bind"]
	if !ok {
		return nil, fmt.Errorf("http receiver is missing `bind` config")
	}

	r := &ReceiverHTTP{
		name:  name,
		ctx:   ctx,
		bind:  bind.(string),
		queue: make(chan *core.Message),
		done:  make(chan struct{}),
	}

	srvmx := http.NewServeMux()
	srvmx.HandleFunc("/v1alpha1", r.handleReqV1alpha1)

	srv := &http.Server{
		Addr:    bind.(string),
		Handler: srvmx,
	}

	r.httpsrv = srv

	return r, nil
}

func (r *ReceiverHTTP) Name() string {
	return r.name
}

func (r *ReceiverHTTP) Start() error {
	go func() {
		if err := r.httpsrv.ListenAndServe(); err != nil {
			switch err {
			case http.ErrServerClosed:
				r.ctx.Logger().Info(err.Error())
			default:
				r.ctx.Logger().Fatal(err.Error())
			}
		}
		close(r.done)
	}()

	return nil
}

func (r *ReceiverHTTP) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()
	defer close(r.queue)
	err := r.httpsrv.Shutdown(ctx)
	<-r.done

	return err
}

func (r *ReceiverHTTP) Connect(nthreads int, peer core.Receiver) error {
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

func (r *ReceiverHTTP) Receive(msg *core.Message) error {
	return fmt.Errorf("http receiver %q can not receive internal messages", r.name)
}

func (r *ReceiverHTTP) handleReqV1alpha1(rw http.ResponseWriter, req *http.Request) {
	cl := req.ContentLength
	if cl == 0 {
		http.Error(rw, "zero size request, ignored", http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		r.ctx.Logger().Error(err.Error())
		http.Error(rw, "Bad request", http.StatusBadRequest)
	}

	msg := core.NewMessage(body)
	for k, v := range req.URL.Query() {
		msg.SetMeta(k, v[0])
	}

	r.queue <- msg

	select {
	case s := <-msg.AwaitChan():
		resp, ok := MsgStatusToHttpResp[s]
		if !ok {
			resp = codetext{http.StatusTeapot, []byte("This should not happen")}
		}
		rw.WriteHeader(resp.code)
		rw.Write(resp.text)
	case <-time.After(HttpMsgSendTimeout):
		rw.WriteHeader(http.StatusGatewayTimeout)
		rw.Write([]byte("Timed out to send message"))
	}
}
