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
	"github.com/awesome-flow/flow/pkg/types"
	log "github.com/sirupsen/logrus"
)

type HTTP struct {
	Name     string
	bindaddr string
	Server   *http.Server
	once     sync.Once
	*core.Connector
}

type codetext struct {
	code int
	text []byte
}

const (
	ShutdownTimeout  = 5 * time.Second
	SendEndpointPath = "/send"
)

const (
	HttpMetricsPref = "receiver.http.msg"

	HttpMetricsConnOpnd        = "receiver.http.conn.received"
	HttpMetricsMsgBadResp      = "receiver.http.msg.bad_request"
	HttpMetricsMsgSendErr      = "receiver.http.msg.send_error"
	HttpMetricsMsgSendAccepted = "receiver.http.msg.accepted"
	HttpMetricsMsgTimeout      = "receiver.http.msg.timeout"
)

var (
	HttpMsgSendTimeout = 100 * time.Millisecond
)

var MsgStatusToHttpResp = map[core.MsgStatus]codetext{
	core.MsgStatusDone:        {http.StatusOK, []byte("OK")},
	core.MsgStatusPartialSend: {http.StatusConflict, []byte("Partial send")},
	core.MsgStatusInvalid:     {http.StatusBadRequest, []byte("Invalid message")},
	core.MsgStatusFailed:      {http.StatusInternalServerError, []byte("Failed to send")},
	core.MsgStatusTimedOut:    {http.StatusGatewayTimeout, []byte("Timed out to send message")},
	core.MsgStatusUnroutable:  {http.StatusNotAcceptable, []byte("Unknown destination")},
	core.MsgStatusThrottled:   {http.StatusTooManyRequests, []byte("Message throttled")},
}

func New(name string, params types.Params, context *core.Context) (core.Link, error) {

	httpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("HTTP parameters are missing bind_addr")
	}

	h := &HTTP{name, httpAddr.(string), nil, sync.Once{}, core.NewConnector()}

	srvMx := http.NewServeMux()
	srvMx.HandleFunc(SendEndpointPath, h.handleSendV1)

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

	metrics.GetCounter(HttpMetricsConnOpnd).Inc(1)

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
		metrics.GetCounter(HttpMetricsMsgBadResp).Inc(1)
		http.Error(rw, "Bad request", http.StatusBadRequest)
		return
	}

	msg := core.NewMessageWithMeta(msgMeta, body)

	if sendErr := h.Send(msg); sendErr != nil {
		metrics.GetCounter(HttpMetricsMsgSendErr).Inc(1)
		http.Error(rw, "Failed to send message", http.StatusInternalServerError)
		return
	}

	if !core.MsgIsSync(msg) {
		metrics.GetCounter(HttpMetricsMsgSendAccepted).Inc(1)
		rw.WriteHeader(http.StatusAccepted)
		rw.Write([]byte("Accepted"))
		return
	}

	select {
	case s := <-msg.AckCh():
		coderesp, ok := MsgStatusToHttpResp[s]
		if !ok {
			coderesp = codetext{http.StatusTeapot, []byte("This should not happen")}
		}
		httpCode, httpResp := coderesp.code, coderesp.text
		metrics.GetCounter(
			HttpMetricsPref + fmt.Sprintf(".resp_%d", httpCode)).Inc(1)
		rw.WriteHeader(httpCode)
		rw.Write(httpResp)
	case <-time.After(HttpMsgSendTimeout):
		metrics.GetCounter(HttpMetricsMsgTimeout).Inc(1)
		rw.WriteHeader(http.StatusGatewayTimeout)
		rw.Write([]byte("Timed out to send message"))
	}
}

func (h *HTTP) String() string {
	return fmt.Sprintf("%s[%s]", h.Name, h.bindaddr)
}
