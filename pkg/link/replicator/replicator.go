package link

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/whiteboxio/flow/pkg/core"
	hash "github.com/whiteboxio/flow/pkg/util/hash"
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
	repl.lock.Lock()
	defer repl.lock.Unlock()
	repl.links = append(repl.links, link)
	return nil
}

func (repl *Replicator) RemoveLink(link core.Link) error {
	panic("Not implemented")
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

func (repl *Replicator) linksForKey(key string) ([]core.Link, error) {
	if repl.replFactor > len(repl.links) {
		return nil, fmt.Errorf("The number of replicas exceeds the number" +
			" of active nodes")
	}

	localLinks := make([]core.Link, len(repl.links))
	res := make([]core.Link, repl.replFactor)
	for ix, link := range repl.links {
		localLinks[ix] = link
	}

	h := hash.Fnv1a64(key)
	cnt := 0
	for i := len(localLinks); i > 0; i-- {
		j := hash.JumpHash(h, i)
		res[cnt] = localLinks[j]
		cnt++
		h ^= h >> 12
		h ^= h << 25
		h ^= h >> 27
		h *= uint64(2685821657736338717)
		localLinks[j] = localLinks[i-1]
	}

	return res, nil
}
