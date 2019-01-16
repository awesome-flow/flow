package link

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"fmt"

	"github.com/DataDog/zstd"
	"github.com/golang/snappy"

	"github.com/awesome-flow/flow/pkg/core"
)

type CoderFunc func([]byte, int) ([]byte, error)

type Compressor struct {
	Name  string
	coder CoderFunc
	level int
	*core.Connector
}

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
		// The final digit is the literal codew width. Varies from 2 to
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

func New(name string, params core.Params, ctx *core.Context) (core.Link, error) {
	var coder CoderFunc
	if algo, ok := params["algo"]; ok {
		coder, ok = Coders[algo.(string)]
		if !ok {
			return nil, fmt.Errorf("Unknown comp algo: %q", algo)
		}
	} else {
		return nil, fmt.Errorf("No algo param specified")
	}
	comp := &Compressor{
		name,
		coder,
		-1,
		core.NewConnectorWithContext(ctx),
	}
	if level, ok := params["level"]; ok {
		comp.level = level.(int)
	}

	return comp, nil
}

func (comp *Compressor) Recv(msg *core.Message) error {
	payload, err := comp.coder(msg.Payload(), comp.level)
	if err != nil {
		return msg.AckFailed()
	}
	msgcp := core.NewMessageWithAckCh(msg.GetAckCh(), msg.GetMetaAll(), payload)

	return comp.Send(msgcp)
}
