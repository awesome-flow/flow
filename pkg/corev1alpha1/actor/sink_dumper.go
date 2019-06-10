package actor

import (
	"bufio"
	"fmt"
	"io"
	"os"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type SinkDumper struct {
	name   string
	ctx    *core.Context
	out    string
	queue  chan *core.Message
	writer *bufio.Writer
}

var _ core.Actor = (*SinkDumper)(nil)

func NewSinkDumper(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	d := &SinkDumper{
		name:  name,
		ctx:   ctx,
		queue: make(chan *core.Message),
	}
	out, ok := params["out"]
	if !ok {
		return nil, fmt.Errorf("dumper is missing 'out' config")
	}
	d.out = out.(string)
	return d, nil
}

func (d *SinkDumper) Name() string {
	return d.name
}

func (d *SinkDumper) Start() error {
	var w io.Writer
	switch d.out {
	case "STDOUT":
		w = os.Stdout
	case "STDERR":
		w = os.Stderr
	default:
		f, err := os.OpenFile(d.out, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		w = f
	}
	d.writer = bufio.NewWriter(w)

	go func() {
		var status core.MsgStatus
		for msg := range d.queue {
			status = core.MsgStatusDone
			if _, err := d.writer.Write(append(msg.Body(), '\n')); err != nil {
				d.ctx.Logger().Error(err.Error())
				status = core.MsgStatusFailed
			} else if err := d.writer.Flush(); err != nil {
				d.ctx.Logger().Error(err.Error())
				status = core.MsgStatusFailed
			}
			msg.Complete(status)
		}
	}()

	return nil
}

func (d *SinkDumper) Stop() error {
	close(d.queue)
	return nil
}

func (d *SinkDumper) Connect(int, core.Receiver) error {
	panic("this component can not connect to other receivers")
}

func (d *SinkDumper) Receive(msg *core.Message) error {
	d.queue <- msg
	return nil
}
