package links

import (
	"booking/msgrelay/flow"
)

type DMX struct {
	Name string
	*flow.Connector
}

func NewDMX(name string, _ flow.Params) (flow.Link, error) {
	dmx := &DMX{
		name,
		flow.NewConnector(),
	}
	return dmx, nil
}

func (dmx *DMX) LinkTo(links []flow.Link) error {
	for _, link := range links {
		if err := link.ConnectTo(dmx); err != nil {
			return err
		}
	}
	return nil
}
