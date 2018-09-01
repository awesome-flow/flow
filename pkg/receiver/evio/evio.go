package receiver

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/tidwall/evio"
	"github.com/whiteboxio/flow/pkg/core"
	"github.com/whiteboxio/flow/pkg/metrics"
)

var (
	RespAcpt = []byte("ACCEPTED")
	RespSent = []byte("SENT")
	RespPsnt = []byte("PART_SENT")
	RespFail = []byte("FAILED")
	RespInvd = []byte("INVALID")
	RespTime = []byte("TIMEOUT")
	RespUnrt = []byte("UNROUTABLE")
	RespThrt = []byte("THROTTLED")

	MsgSendTimeout = time.Second
)

type Evio struct {
	Name   string
	events *evio.Events
	*core.Connector
}

func New(name string, params core.Params) (core.Link, error) {

	events := &evio.Events{}

	if numLoops, ok := params["num_loops"]; ok {
		events.NumLoops = numLoops.(int)
	} else {
		events.NumLoops = -1 // sets to GOMAXPROCS
	}

	listIntf, ok := params["listeners"]
	if !ok {
		return nil, fmt.Errorf("Failed to initialize evio: missing listeners")
	}

	log.Infof(
		"Starting Evio receiver. Listeners: %+v",
		params["listeners"].([]interface{}),
	)

	ev := &Evio{
		name,
		events,
		core.NewConnector(),
	}

	events.Opened = func(ec evio.Conn) (out []byte, opts evio.Options, action evio.Action) {
		metrics.GetCounter("receiver.evio.conn.opened").Inc(1)
		ec.SetContext(&evio.InputStream{})
		return
	}

	events.Closed = func(c evio.Conn, err error) (action evio.Action) {
		metrics.GetCounter("receiver.evio.conn.closed").Inc(1)
		return
	}

	events.Data = func(ec evio.Conn, buf []byte) (out []byte, action evio.Action) {
		is, ok := ec.Context().(*evio.InputStream)
		if !ok {
			is = &evio.InputStream{}
		}
		data := is.Begin(buf)

		if !bytes.Contains(data, []byte{'\r', '\n'}) {
			is.End(data)
			return
		}
		syncAllowed, err := regexp.Match("^tcp*", []byte(ec.LocalAddr().Network()))
		if err != nil {
			log.Errorf("Failed to match connection network against regex: %s", err)
		}

		chunks := bytes.SplitN(data, []byte{'\r', '\n'}, 2)

		payload, leftover := chunks[0], chunks[1]

		if len(payload) > 0 {
			metrics.GetCounter("receiver.evio.msg.received").Inc(1)
			msg := core.NewMessage(payload)

			if err := ev.Send(msg); err != nil {
				log.Errorf("Evio receiver failed to send message: %s", err)
				metrics.GetCounter("receiver.evio.msg.failed").Inc(1)
				out = RespFail
				return
			}
			sync, ok := msg.GetMeta("sync")
			isSync := ok && (sync.(string) == "true" || sync.(string) == "1")
			if isSync && syncAllowed {
				select {
				case s := <-msg.GetAckCh():
					metrics.GetCounter(
						"receiver.evio.msg.sent_" + strings.ToLower(string(status2resp(s)))).Inc(1)
					out = status2resp(s)
				case <-time.After(MsgSendTimeout):
					metrics.GetCounter("receiver.evio.msg.timed_out").Inc(1)
					out = RespTime
				}
			} else {
				metrics.GetCounter("receiver.evio.msg.accepted").Inc(1)
				out = RespAcpt
			}
			return
		}

		data = leftover
		is.End(data)

		return
	}

	listeners := make([]string, len(listIntf.([]interface{})))
	for ix, li := range listIntf.([]interface{}) {
		listeners[ix] = li.(string)
	}
	go func() {
		if err := evio.Serve(*events, listeners...); err != nil {
			log.Fatalf("Failed to start evio: %s", err)
		}
	}()

	return ev, nil
}

func status2resp(s core.MsgStatus) []byte {
	switch s {
	case core.MsgStatusDone:
		return RespSent
	case core.MsgStatusPartialSend:
		return RespPsnt
	case core.MsgStatusInvalid:
		return RespInvd
	case core.MsgStatusFailed:
		return RespFail
	case core.MsgStatusTimedOut:
		return RespTime
	case core.MsgStatusUnroutable:
		return RespUnrt
	case core.MsgStatusThrottled:
		return RespThrt
	default:
		return []byte("This should not happen")
	}
}
