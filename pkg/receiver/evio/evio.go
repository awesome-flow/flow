package receiver

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/metrics"
	"github.com/tidwall/evio"
)

type transpMode uint8

const (
	transpModeSilent transpMode = iota
	transpModeTalkative
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
	Name      string
	mode      transpMode
	listeners []string
	events    *evio.Events
	once      sync.Once
	lasterr   error
	*core.Connector
}

func New(name string, params core.Params, context *core.Context) (core.Link, error) {

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
	listeners := make([]string, 0, len(listIntf.([]interface{})))
	for _, li := range listIntf.([]interface{}) {
		listeners = append(listeners, li.(string))
	}

	log.Infof(
		"Starting Evio receiver. Listeners: %+v",
		params["listeners"].([]interface{}),
	)

	mode := transpModeTalkative
	if alterMode, ok := params["mode"]; ok {
		switch alterMode {
		case "silent":
			mode = transpModeSilent
		case "talkative":
			mode = transpModeTalkative
		}
	}

	ev := &Evio{
		name,
		mode,
		listeners,
		events,
		sync.Once{},
		nil,
		core.NewConnector(),
	}

	events.Opened = func(ec evio.Conn) (out []byte, opts evio.Options, action evio.Action) {
		log.Infof("Opened a new connection: %s", ec.RemoteAddr().Network())
		metrics.GetCounter("receiver.evio.conn.opened").Inc(1)
		ec.SetContext(&evio.InputStream{})
		return
	}

	events.Closed = func(c evio.Conn, err error) (action evio.Action) {
		log.Infof("Closed connection: %s", c.RemoteAddr().Network())
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

		replySupported, err := regexp.Match("^tcp*", []byte(ec.LocalAddr().Network()))
		if err != nil {
			log.Errorf("Failed to match connection network against regex: %s", err)
		}

		chunks := bytes.Split(data, []byte{'\r', '\n'})
		// The last chank will contain either an empty array or a leftover from
		// the next packet.
		chunks, leftover := chunks[:len(chunks)-1], chunks[len(chunks)-1]

		for _, payload := range chunks {
			if len(payload) > 0 {
				metrics.GetCounter("receiver.evio.msg.received").Inc(1)
				msg := core.NewMessage(payload)

				if err := ev.Send(msg); err != nil {
					log.Errorf("Evio receiver failed to send message: %s", err)
					metrics.GetCounter("receiver.evio.msg.failed").Inc(1)
					if replySupported {
						out = RespFail
					}
					continue
				}
				sync, ok := msg.Meta("sync")
				isSync := ok && (sync.(string) == "true" || sync.(string) == "1")
				if isSync && replySupported {
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
					if replySupported {
						out = RespAcpt
					}
				}
			}
		}

		data = leftover
		is.End(data)

		return
	}

	return ev, nil
}

func (ev *Evio) ExecCmd(cmd *core.Cmd) error {
	switch cmd.Code {
	case core.CmdCodeStart:
		return ev.Connect()
	default:
		return nil
	}
}

func (ev *Evio) Connect() error {
	ev.once.Do(func() {
		ev.lasterr = nil
		go func() {
			if err := evio.Serve(*ev.events, ev.listeners...); err != nil {
				ev.lasterr = fmt.Errorf("Failed to start evio: %s", err)
				return
			}
		}()
	})

	return ev.lasterr
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
