package x

import (
	"bytes"
	"net/url"
	"strings"
	"sync"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

const (
	MetaDelimByte = ' '
)

type MetaParser struct {
	name  string
	ctx   *core.Context
	queue chan *core.Message
	wg    sync.WaitGroup
}

var _ core.Actor = (*MetaParser)(nil)

func NewMetaParser(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &MetaParser{
		name:  name,
		ctx:   ctx,
		queue: make(chan *core.Message),
	}, nil
}

func (m *MetaParser) Name() string {
	return m.name
}

func (m *MetaParser) Start() error {
	return nil
}

func (m *MetaParser) Stop() error {
	close(m.queue)
	m.wg.Wait()
	return nil
}

func (m *MetaParser) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		m.wg.Add(1)
		go func() {
			for msg := range m.queue {
				if err := peer.Receive(msg); err != nil {
					msg.Complete(core.MsgStatusFailed)
					m.ctx.Logger().Error("meta parser %q failed to send message: %s", m.name, err)
				}
			}
			m.wg.Done()
		}()
	}
	return nil
}

func (m *MetaParser) Receive(msg *core.Message) error {
	parsed, err := parseMsgMeta(msg)
	if err != nil {
		return err
	}
	m.queue <- parsed
	return nil
}

func parseMsgMeta(msg *core.Message) (*core.Message, error) {
	fullbody := msg.Body()
	spix := bytes.IndexByte(fullbody, MetaDelimByte)
	if spix == -1 {
		return msg, nil
	}
	meta, body := fullbody[:spix], fullbody[spix+1:]
	values, err := url.ParseQuery(string(meta))
	if err != nil {
		return nil, err
	}
	for k, vs := range values {
		msg.SetMeta(k, strings.Join(vs, ","))
	}
	msg.SetBody(body)
	return msg, nil
}
