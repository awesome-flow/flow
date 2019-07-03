package actor

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"fmt"
	"sync"

	"github.com/DataDog/zstd"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/golang/snappy"
)

type CoderFunc func([]byte, int) ([]byte, error)

var DefaultCoders = map[string]CoderFunc{
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
	name     string
	ctx      *core.Context
	coder    CoderFunc
	level    int
	queueIn  chan *core.Message
	queueOut chan *core.Message
	done     chan struct{}
}

var _ core.Actor = (*Compressor)(nil)

func NewCompressor(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return NewCompressorWithCoders(name, ctx, params, DefaultCoders)
}

func NewCompressorWithCoders(name string, ctx *core.Context, params core.Params, coders map[string]CoderFunc) (core.Actor, error) {
	alg, ok := params["compress"]
	if !ok {
		return nil, fmt.Errorf("compressor %q is missing `compress` config", name)
	}
	coder, ok := coders[alg.(string)]
	if !ok {
		return nil, fmt.Errorf("compressor %q: unknown compression algorithm %q", name, alg)
	}
	level := -1
	if l, ok := params["level"]; ok {
		if _, ok := l.(int); !ok {
			return nil, fmt.Errorf("compressor %q: malformed compression level provided: got: %+v, want: an integer", name, l)
		}
		level = l.(int)
	}

	return &Compressor{
		name:     name,
		ctx:      ctx,
		coder:    coder,
		level:    level,
		queueIn:  make(chan *core.Message),
		queueOut: make(chan *core.Message),
		done:     make(chan struct{}),
	}, nil
}

func (c *Compressor) Name() string {
	return c.name
}

func (c *Compressor) Start() error {
	nthreads, ok := c.ctx.Config().Get(types.NewKey("system.maxprocs"))
	if !ok {
		return fmt.Errorf("failed to fetch `system.maxprocs` config")
	}
	for i := 0; i < nthreads.(int); i++ {
		go func() {
			for msg := range c.queueIn {
				data, err := c.coder(msg.Body(), c.level)
				if err != nil {
					msg.Complete(core.MsgStatusFailed)
					c.ctx.Logger().Error(err.Error())
					continue
				}
				cpmsg := core.NewMessage(data)
				for _, k := range msg.MetaKeys() {
					if v, ok := msg.Meta(k); ok {
						cpmsg.SetMeta(k, v)
					}
				}
				c.queueOut <- cpmsg
				s := cpmsg.Await()
				msg.Complete(s)
			}
		}()
	}

	return nil
}

func (c *Compressor) Stop() error {
	close(c.queueIn)
	close(c.queueOut)
	<-c.done

	return nil
}

func (c *Compressor) Connect(nthreads int, peer core.Receiver) error {
	var once sync.Once
	closedone := func() {
		close(c.done)
	}

	for i := 0; i < nthreads; i++ {
		go func() {
			for msg := range c.queueOut {
				if err := peer.Receive(msg); err != nil {
					c.ctx.Logger().Error(err.Error())
				}
			}
			once.Do(closedone)
		}()
	}

	return nil
}

func (c *Compressor) Receive(msg *core.Message) error {
	c.queueIn <- msg

	return nil
}
