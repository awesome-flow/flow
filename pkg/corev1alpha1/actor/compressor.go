package actor

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"

	"github.com/DataDog/zstd"
	"github.com/golang/snappy"

	"github.com/awesome-flow/flow/pkg/core"
)

type CoderFunc func([]byte, int) ([]byte, error)

var Coders = map[string]CoderFunc{
	"gzip": func(payload []byte, level int) ([]byte, error) {
		var b bytes.Buffer
		w, err := gzip.NewWriterLevel(&b, level)
		if err != nil {
			return nil, err
		}
		if _, err := w.Write(payload); err != nil {
			return nil, err
		}
		w.Close()
		return b.Bytes(), nil
	},
	"flate": func(payload []byte, level int) ([]byte, error) {
		var b bytes.Buffer
		w, err := flate.NewWriter(&b, level)
		if err != nil {
			return nil, err
		}
		if _, err := w.Write(payload); err != nil {
			return nil, err
		}
		w.Close()
		return b.Bytes(), nil
	},
	"lzw": func(payload []byte, _ int) ([]byte, error) {
		var b bytes.Buffer
		// The final digit is the literal coder width. Varies from 2 to
		// 8 bits. We are using 8 by default here.
		// See https://golang.org/src/compress/lzw/writer.go#L241
		// for more details.
		w := lzw.NewWriter(&b, lzw.MSB, 8)
		if _, err := w.Write(payload); err != nil {
			return nil, err
		}
		w.Close()
		return b.Bytes(), nil
	},
	"zlib": func(payload []byte, level int) ([]byte, error) {
		var b bytes.Buffer
		w, err := zlib.NewWriterLevel(&b, level)
		if err != nil {
			return nil, err
		}
		if _, err := w.Write(payload); err != nil {
			return nil, err
		}
		w.Close()
		return b.Bytes(), nil
	},
	"zstd": func(payload []byte, level int) ([]byte, error) {
		var b bytes.Buffer
		w := zstd.NewWriterLevel(&b, level)
		if _, err := w.Write(payload); err != nil {
			return nil, err
		}
		w.Close()
		return b.Bytes(), nil
	},
	"snappy": func(payload []byte, _ int) ([]byte, error) {
		var b bytes.Buffer
		w := snappy.NewBufferedWriter(&b)
		if _, err := w.Write(payload); err != nil {
			return nil, err
		}
		w.Close()
		return b.Bytes(), nil
	},
}

type Compressor struct {
	name  string
	ctx   *core.Context
	queue chan *core.Message
	coder CoderFunc
	level int
}

var _ core.Actor = (*Compressor)(nil)

func NewCompressor(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	alg, ok := params["compress"]
	if !ok {
		return nil, fmt.Errorf("compressor %q is missing `compress` config", name)
	}
	coder, ok := Coders[alg]
	if !ok {
		return nil, fmt.Errorf("compressor %q failed to initialize: unknown compression algorithm %q", name, alg)
	}
	level := -1
	if l, ok := params["level"]; ok {
		level = l.(int)
	}
	return &Compressor{
		name:  name,
		ctx:   ctx,
		queue: make(chan *core.Message),
		code:  coder,
		level: level,
	}, nil
}

func (c *Compressor) Name() string {
	return c.name
}

func (c *Compressor) Start() error {
	return nil
}

func (c *Compressor) Stop() error {
	close(c.queue)
	return nil
}

func (c *Compressor) Connect(nthreads int, peer core.Receiver) (core.Actor, error) {
	for i := 0; i < nthreads; i++ {
		go func() {
			for msg := range c.queue {
				if err := peer.Receive(msg); err != nil {
					c.ctx.Logger().Error(err.Error())
				}
			}
		}()
	}

	return nil
}

func (c *Compressor) Receive(msg *core.Message) error {
	data, err := c.coder(msg.Body(), c.level)
	if err != nil {
		msg.Complete(core.MsgStatusFailed)
		return err
	}
	cpmsg := core.NewMessage(data)
	for _, k := range msg.MetaKeys() {
		if v, ok := msg.GetMeta(k); ok {
			cpmsg.SetMeta(k, v)
		}
	}
	c.queue <- msg
	return nil
}
