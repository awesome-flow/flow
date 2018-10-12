package link

import (
	"bytes"
	"net/url"

	"github.com/whiteboxio/flow/pkg/core"
)

type MetaParser struct {
	Name string
	*core.Connector
}

func New(name string, params core.Params, context *core.Context) (core.Link, error) {
	return &MetaParser{
		name,
		core.NewConnector(),
	}, nil
}

func (mp *MetaParser) Recv(msg *core.Message) error {
	if bytes.ContainsRune(msg.Payload, ' ') {
		chunks := bytes.SplitN(msg.Payload, []byte{' '}, 2)
		query, payload := chunks[0], chunks[1]

		queryPairs, err := url.ParseQuery(string(query))

		if err != nil {
			return err
		}
		msgMeta := make(map[string]interface{})
		for k, vals := range queryPairs {
			msgMeta[k] = vals[0]
		}
		msg.SetMetaAll(msgMeta)
		msg.Payload = payload
	}

	return mp.Send(msg)
}
