package receiver

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/whiteboxio/flow/pkg/core"
	"github.com/whiteboxio/flow/pkg/metrics"
)

var (
	HttpMsgSendTimeout = 100 * time.Millisecond
)

type HTTP struct {
	Name   string
	Server *http.Server
	*core.Connector
}

func NewHTTP(name string, params core.Params) (core.Link, error) {

	httpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("HTTP parameters are missing bind_addr")
	}

	h := &HTTP{name, nil, core.NewConnector()}

	srvMx := http.NewServeMux()
	srvMx.HandleFunc("/send", func(rw http.ResponseWriter, req *http.Request) {
		h.handleSendV1(rw, req)
	})

	srv := &http.Server{
		Addr:    httpAddr.(string),
		Handler: srvMx,
		// ReadTimeout: from params,
		// WriteTimeout: from params,
		// MaxHeaderBytes: from params
	}
	h.Server = srv

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			switch err {
			case http.ErrServerClosed:
				log.Info(err.Error())
			default:
				panic(fmt.Sprintf("HTTP server critical error: %s", err))
			}

		}
	}()

	return h, nil
}

func (h *HTTP) ExecCmd(cmd *core.Cmd) error {
	switch cmd.Code {
	case core.CmdCodeStop:
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		return h.Server.Shutdown(ctx)
	}
	return nil
}

func (h *HTTP) handleSendV1(rw http.ResponseWriter, req *http.Request) {

	metrics.GetCounter("receiver.http.received").Inc(1)

	cl := req.ContentLength
	if cl <= 0 {
		http.Error(rw, "Zero-size request size", http.StatusBadRequest)
		return
	}
	msgMeta := core.NewMsgMeta()
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

	msg := core.NewMessage(msgMeta, body)

	if sendErr := h.Send(msg); sendErr != nil {
		metrics.GetCounter("receiver.http.send_error").Inc(1)
		http.Error(rw, "Failed to send message", http.StatusInternalServerError)
		return
	}

	if !msg.IsSync() {
		metrics.GetCounter("receiver.http.accepted").Inc(1)
		rw.WriteHeader(http.StatusAccepted)
		rw.Write([]byte("Accepted"))
		return
	}

	select {
	case s := <-msg.GetAckCh():
		fmt.Printf("Received a message status update\n")
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
