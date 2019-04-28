package link

import (
	"bytes"
	"net/url"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/types"
)

type MetaParser struct {
	Name string
	*core.Connector
}

func New(name string, params types.Params, context *core.Context) (core.Link, error) {
	return &MetaParser{
		name,
		core.NewConnector(),
	}, nil
}

func (mp *MetaParser) Recv(msg *core.Message) error {
	if bytes.ContainsRune(msg.Payload(), ' ') {
		chunks := bytes.SplitN(msg.Payload(), []byte{' '}, 2)
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
		msg.SetPayload(payload)
	}

	return mp.Send(msg)
}
