package x

import (
	"encoding/base64"
	"sync"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type DecoderBase64 struct {
	name     string
	ctx      *core.Context
	queue    chan *core.Message
	wg       sync.WaitGroup
	encoding *base64.Encoding
}

var _ core.Actor = (*DecoderBase64)(nil)

func NewDecoderBase64(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &DecoderBase64{
		name:     name,
		ctx:      ctx,
		queue:    make(chan *core.Message, 1),
		encoding: base64.StdEncoding,
	}, nil
}

func (d *DecoderBase64) Name() string {
	return d.name
}

func (d *DecoderBase64) Start() error {
	return nil
}

func (d *DecoderBase64) Stop() error {
	close(d.queue)
	d.wg.Wait()
	return nil
}

func (d *DecoderBase64) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		d.wg.Add(1)
		go func() {
			for msg := range d.queue {
				if err := peer.Receive(msg); err != nil {
					d.ctx.Logger().Error("base64 decoder %q failed to send message to receiver: %s", d.name, err)
				}
			}
			d.wg.Done()
		}()
	}
	return nil
}

func (d *DecoderBase64) Receive(msg *core.Message) error {
	decbody, err := d.decodeBase64(msg.Body())
	if err != nil {
		return err
	}
	msg.SetBody(decbody)
	d.queue <- msg
	return nil
}

func (d *DecoderBase64) decodeBase64(data []byte) ([]byte, error) {
	newlen := d.encoding.DecodedLen(len(data))
	out := make([]byte, newlen)
	n, err := d.encoding.Decode(out, data)
	if err != nil {
		return nil, err
	}
	return out[:n], nil
}
