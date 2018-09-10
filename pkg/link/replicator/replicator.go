package link

import (
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/whiteboxio/flow/pkg/core"
)

type Replicator struct {
	Name       string
	nBuckets   uint32
	replFactor int
	hashKey    string
	links      []core.Link
	lock       *sync.Mutex
	*core.Connector
}

func New(name string, params core.Params) (core.Link, error) {

	nBuckets := uint32(2 ^ 32 - 1)

	repl := &Replicator{
		name,
		nBuckets,
		3,
		"", // "" stands for message body as a hashing key
		make([]core.Link, 0),
		&sync.Mutex{},
		core.NewConnector(),
	}

	if nBuckets, ok := params["n_buckets"]; ok {
		repl.nBuckets = uint32(nBuckets.(int))
	}

	if replFactor, ok := params["repl_factor"]; ok {
		repl.replFactor = replFactor.(int)
	}

	if hashKey, ok := params["hash_key"]; ok {
		repl.hashKey = hashKey.(string)
	}

	return repl, nil
}

func (repl *Replicator) LinkTo(links []core.Link) error {
	for _, link := range links {
		if err := repl.AddLink(link); err != nil {
			return err
		}
	}
	return nil
}

func (repl *Replicator) AddLink(link core.Link) error {
	// TODO
	return nil
}

func (repl *Replicator) RemoveLink(link core.Link) error {
	//TODO
	return nil
}

func (repl *Replicator) replicate() {
	var msgKey []byte
	for msg := range repl.GetMsgCh() {
		if repl.hashKey == "" {
			msgKey = msg.Payload
		} else {
			if v, ok := msg.GetMeta(repl.hashKey); ok {
				if vConv, convOk := v.([]byte); convOk {
					msgKey = vConv
				} else {
					logrus.Errorf("Msg key %s found: %+v, but could not be converted"+
						" to []byte", repl.hashKey, v)
					continue
				}
			} else {
				logrus.Errorf("Msg key %s could not be found in message %+v",
					repl.hashKey, msg)
				continue
			}
		}
		logrus.Infof("A new message received: %+v, msg key: ", msg, msgKey)
		//TODO
	}
}
