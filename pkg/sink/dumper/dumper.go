package links

import (
	"booking/msgrelay/flow"
	"bufio"
	"fmt"
	"os"
)

type Dumper struct {
	Name string
	*bufio.Writer
	*flow.Connector
}

func NewDumper(name string, params flow.Params) (flow.Link, error) {
	out, outOk := params["out"]
	if !outOk {
		return nil, fmt.Errorf("Dumper %s params are missing out", name)
	}
	var writer *bufio.Writer
	switch out.(string) {
	case "STDOUT":
		writer = bufio.NewWriter(os.Stdout)
	case "STDERR":
		writer = bufio.NewWriter(os.Stderr)
	default:
		f, err := os.OpenFile(out.(string), os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf(
				"Failed to open %s out to %s: %s", name, out, err.Error())
		}
		writer = bufio.NewWriter(f)
	}

	return &Dumper{name, writer, flow.NewConnector()}, nil
}

func (d *Dumper) Recv(msg *flow.Message) error {
	d.Write([]byte(fmt.Sprintf("Message:\n"+
		"    meta: %+v\n"+
		"    payload: %s\n", msg.Meta, msg.Payload)))
	if flushErr := d.Flush(); flushErr != nil {
		return msg.AckFailed()
	}
	return msg.AckDone()
}

func (d *Dumper) Send(*flow.Message) error {
	panic("Dumper is not suppsed to send messages")
}

func (d *Dumper) ConnectTo(flow.Link) error {
	panic("Dumper is not supposed to be connected to other links")
}
