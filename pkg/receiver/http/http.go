package receiver

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/whiteboxio/msgrelay/pkg/data"
	"github.com/whiteboxio/msgrelay/pkg/flow"

	"github.com/facebookgo/grace/gracemulti"
)

var (
	HttpMsgSendTimeout = 100 * time.Millisecond
)

type HTTP struct {
	Name   string
	Server *http.Server
	*flow.Connector
}

func NewHTTP(name string, params flow.Params) (flow.Link, error) {

	httpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("HTTP parameters are missing bind_addr")
	}

	h := &HTTP{name, nil, flow.NewConnector()}

	srvMx := http.NewServeMux()
	srv := &http.Server{
		Addr:    httpAddr.(string),
		Handler: srvMx,
	}

	h.Server = srv

	srvMx.HandleFunc(versionedPath("api", 1, "send"), func(rw http.ResponseWriter, req *http.Request) {
		h.handleSendV1(rw, req)
	})

	var servers gracemulti.MultiServer
	servers.HTTP = append(servers.HTTP, h.Server)
	go func() {
		grcErr := gracemulti.Serve(servers)
		if grcErr != nil {
			tell.Fatalf("Failed to start gracemulti servers: %s", grcErr.Error())
		}
	}()

	return h, nil
}

func (h *HTTP) ExecCmd(cmd *flow.Cmd) error {
	switch cmd.Code {
	case flow.CmdCodeStop:
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		return h.Server.Shutdown(ctx)
	}
	return nil
}

func (h *HTTP) handleSendV1(rw http.ResponseWriter, req *http.Request) {

	bmetrics.GetOrRegisterCounter("receiver", "http", "received").Inc(1)

	cl := req.ContentLength
	if cl <= 0 {
		http.Error(rw, "Zero-size request size", http.StatusBadRequest)
		return
	}
	msgMeta := flow.NewMsgMeta()
	if parseErr := util.ParseQuery(msgMeta, req.URL.RawQuery); parseErr != nil {
		bmetrics.GetOrRegisterCounter("receiver", "http", "bad_query").Inc(1)
		http.Error(rw, "Bad query", http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		bmetrics.GetOrRegisterCounter("receiver", "http", "bad_request").Inc(1)
		http.Error(rw, "Bad request", http.StatusBadRequest)
		return
	}

	msg := flow.NewMessage(msgMeta, body)

	if sendErr := h.Send(msg); sendErr != nil {
		bmetrics.GetOrRegisterCounter("receiver", "http", "send_error").Inc(1)
		http.Error(rw, "Failed to send message", http.StatusInternalServerError)
		return
	}

	if !msg.IsSync() {
		bmetrics.GetOrRegisterCounter("receiver", "http", "accepted").Inc(1)
		rw.WriteHeader(http.StatusAccepted)
		rw.Write([]byte("Accepted"))
		return
	}

	select {
	case s := <-msg.GetAckCh():
		httpCode, httpResp := status2resp(s)
		bmetrics.GetOrRegisterCounter(
			"receiver", "http", fmt.Sprintf("ack_%d", httpCode)).Inc(1)
		rw.WriteHeader(httpCode)
		rw.Write(httpResp)
	case <-time.After(HttpMsgSendTimeout):
		bmetrics.GetOrRegisterCounter("receiver", "http", "timeout").Inc(1)
		rw.WriteHeader(http.StatusGatewayTimeout)
		rw.Write([]byte("Timed out to send message"))
	}
}

func status2resp(s flow.MsgStatus) (int, []byte) {
	switch s {
	case flow.MsgStatusDone:
		return http.StatusOK, []byte("OK")
	case flow.MsgStatusPartialSend:
		return http.StatusConflict, []byte("Partial send")
	case flow.MsgStatusInvalid:
		return http.StatusBadRequest, []byte("Invalid message")
	case flow.MsgStatusFailed:
		return http.StatusInternalServerError, []byte("Failed to send")
	case flow.MsgStatusTimedOut:
		return http.StatusGatewayTimeout, []byte("Timed out to send message")
	case flow.MsgStatusUnroutable:
		return http.StatusNotAcceptable, []byte("Unknown destination")
	case flow.MsgStatusThrottled:
		return http.StatusTooManyRequests, []byte("Message throttled")
	default:
		return http.StatusTeapot, []byte("OlegS screwed, this should not happen")
	}
}

func versionedPath(preffix string, version int, path string) string {
	if preffix[0] != '/' {
		preffix = "/" + preffix
	}
	if path[0] != '/' {
		path = "/" + path
	}
	return fmt.Sprintf("%s/v%d%s", preffix, version, path)
}
