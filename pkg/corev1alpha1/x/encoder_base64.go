package x

import (
	"encoding/base64"
	"sync"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type EncoderBase64 struct {
	name     string
	ctx      *core.Context
	queue    chan *core.Message
	wg       sync.WaitGroup
	encoding *base64.Encoding
}

var _ core.Actor = (*EncoderBase64)(nil)

func NewEncoderBase64(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &EncoderBase64{
		name:     name,
		ctx:      ctx,
		queue:    make(chan *core.Message, 1),
		encoding: base64.StdEncoding,
	}, nil
}

func (e *EncoderBase64) Name() string {
	return e.name
}

func (e *EncoderBase64) Start() error {
	return nil
}

func (e *EncoderBase64) Stop() error {
	close(e.queue)
	e.wg.Wait()
	return nil
}

func (e *EncoderBase64) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		e.wg.Add(1)
		go func() {
			for msg := range e.queue {
				if err := peer.Receive(msg); err != nil {
					e.ctx.Logger().Error("base64 encoder %q failed to send message to receiver: %s", e.name, err)
				}
			}
			e.wg.Done()
		}()
	}
	return nil
}

func (e *EncoderBase64) Receive(msg *core.Message) error {
	encbody := e.encodeBase64(msg.Body())
	msg.SetBody(encbody)
	e.queue <- msg
	return nil
}

func (e *EncoderBase64) encodeBase64(data []byte) []byte {
	newlen := e.encoding.EncodedLen(len(data))
	out := make([]byte, newlen)
	e.encoding.Encode(out, data)
	return out
}
