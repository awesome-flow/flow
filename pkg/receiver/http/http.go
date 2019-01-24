package receiver

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/metrics"
	log "github.com/sirupsen/logrus"
)

var (
	HttpMsgSendTimeout = 100 * time.Millisecond
)

type HTTP struct {
	Name     string
	bindaddr string
	Server   *http.Server
	once     sync.Once
	*core.Connector
}

const (
	ShutdownTimeout = 5 * time.Second
)

func New(name string, params core.Params, context *core.Context) (core.Link, error) {

	httpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("HTTP parameters are missing bind_addr")
	}

	h := &HTTP{name, httpAddr.(string), nil, sync.Once{}, core.NewConnector()}

	srvMx := http.NewServeMux()
	srvMx.HandleFunc("/send", func(rw http.ResponseWriter, req *http.Request) {
		h.handleSendV1(rw, req)
	})

	srv := &http.Server{
		Addr:    httpAddr.(string),
		Handler: srvMx,
	}
	h.Server = srv

	h.OnSetUp(h.SetUp)
	h.OnTearDown(h.TearDown)

	return h, nil
}

func (h *HTTP) SetUp() error {
	go func() {
		if err := h.Server.ListenAndServe(); err != nil {
			switch err {
			case http.ErrServerClosed:
				log.Info(err.Error())
			default:
				panic(fmt.Sprintf("HTTP server critical error: %s", err))
			}
		}
	}()

	return nil
}

func (h *HTTP) TearDown() error {
	ctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()
	return h.Server.Shutdown(ctx)
}

func (h *HTTP) handleSendV1(rw http.ResponseWriter, req *http.Request) {

	metrics.GetCounter("receiver.http.received").Inc(1)

	cl := req.ContentLength
	if cl <= 0 {
		http.Error(rw, "Zero-size request size", http.StatusBadRequest)
		return
	}
	msgMeta := make(map[string]interface{})
	for k, v := range req.URL.Query() {
		msgMeta[k] = v[0]
	}

	body, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		metrics.GetCounter("receiver.http.bad_request").Inc(1)
		http.Error(rw, "Bad request", http.StatusBadRequest)
		return
	}

	msg := core.NewMessageWithMeta(msgMeta, body)

	if sendErr := h.Send(msg); sendErr != nil {
		metrics.GetCounter("receiver.http.send_error").Inc(1)
		http.Error(rw, "Failed to send message", http.StatusInternalServerError)
		return
	}

	if !core.MsgIsSync(msg) {
		metrics.GetCounter("receiver.http.accepted").Inc(1)
		rw.WriteHeader(http.StatusAccepted)
		rw.Write([]byte("Accepted"))
		return
	}

	select {
	case s := <-msg.GetAckCh():
		httpCode, httpResp := status2resp(s)
		metrics.GetCounter(
			"receiver.http." + fmt.Sprintf("ack_%d", httpCode)).Inc(1)
		rw.WriteHeader(httpCode)
		rw.Write(httpResp)
	case <-time.After(HttpMsgSendTimeout):
		metrics.GetCounter("receiver.http.timeout").Inc(1)
		rw.WriteHeader(http.StatusGatewayTimeout)
		rw.Write([]byte("Timed out to send message"))
	}
}

func status2resp(s core.MsgStatus) (int, []byte) {
	switch s {
	case core.MsgStatusDone:
		return http.StatusOK, []byte("OK")
	case core.MsgStatusPartialSend:
		return http.StatusConflict, []byte("Partial send")
	case core.MsgStatusInvalid:
		return http.StatusBadRequest, []byte("Invalid message")
	case core.MsgStatusFailed:
		return http.StatusInternalServerError, []byte("Failed to send")
	case core.MsgStatusTimedOut:
		return http.StatusGatewayTimeout, []byte("Timed out to send message")
	case core.MsgStatusUnroutable:
		return http.StatusNotAcceptable, []byte("Unknown destination")
	case core.MsgStatusThrottled:
		return http.StatusTooManyRequests, []byte("Message throttled")
	default:
		return http.StatusTeapot, []byte("This should not happen")
	}
}

func (h *HTTP) String() string {
	return fmt.Sprintf("%s[%s]", h.Name, h.bindaddr)
}
