package actor

import (
	"fmt"
	"io"
	"os"
)

type FileOpener func(string) (io.WriteCloser, error)

var DefaultFileOpener = func(path string) (io.WriteCloser, error) {
	switch path {
	case "STDOUT":
		return os.Stdout, nil
	case "STDERR":
		return os.Stderr, nil
	}
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
}

type SinkHeadFile struct {
	path string
	out  io.WriteCloser

	Opener FileOpener
}

var _ (SinkHead) = (*SinkHeadFile)(nil)

func NewSinkHeadFile(path string) (*SinkHeadFile, error) {
	return &SinkHeadFile{
		path:   path,
		Opener: DefaultFileOpener,
	}, nil
}

func (h *SinkHeadFile) Connect() error {
	out, err := h.Opener(h.path)
	if err != nil {
		return err
	}
	h.out = out

	return nil
}

func (h *SinkHeadFile) Stop() error {
	if h.out != nil {
		if h.out != os.Stdin && h.out != os.Stdout {
			return h.out.Close()
		}
	}
	return nil
}

func (h *SinkHeadFile) Start() error {
	return nil
}

func (h *SinkHeadFile) Write(data []byte) (int, error, bool) {
	if h.out == nil {
		return 0, fmt.Errorf("sink head out file is nil"), true
	}
	payload := make([]byte, len(data)+2)
	copy(payload, data)
	copy(payload[len(data):], []byte("\r\n"))
	n, err := h.out.Write(payload)
	if err != nil {
		return 0, err, true
	}

	return n, nil, false
}
