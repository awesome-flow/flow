package link

import (
	"fmt"
	"hash/fnv"
	"runtime"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/whiteboxio/flow/pkg/core"
	demux "github.com/whiteboxio/flow/pkg/link/demux"
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

const (
	ReplMsgSendTimeout = 50 * time.Millisecond
)

func New(name string, params core.Params, context *core.Context) (core.Link, error) {

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

	if replFactor, ok := params["replicas"]; ok {
		repl.replFactor = replFactor.(int)
	}

	if hashKey, ok := params["hash_key"]; ok {
		repl.hashKey = hashKey.(string)
	}

	threadiness := runtime.GOMAXPROCS(-1)

	log.Infof("Starting replicator with threadiness %d", threadiness)
	for i := 0; i < threadiness; i++ {
		go repl.replicate()
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
					log.Errorf("Msg key %s found: %+v, but could not be"+
						" converted to []byte", repl.hashKey, v)
					continue
				}
			} else {
				log.Errorf("Msg key %s could not be found in message %+v",
					repl.hashKey, msg)
				continue
			}
		}

		linksIxs, err := repl.linksIxsForKey(msgKey)
		if err != nil {
			log.Errorf("Failed to get a list of links for key %s: %s", msgKey, err)
		}

		if err := demux.Demultiplex(msg, linksIxs, repl.links, ReplMsgSendTimeout); err != nil {
			log.Errorf("Replicator failed to send message: %q", err)
		}
	}
}

func (repl *Replicator) linksIxsForKey(key []byte) (uint64, error) {
	mask := demux.DemuxMaskNone

	if len(repl.links) > 64 {
		return mask, fmt.Errorf("The current version of replicator does not" +
			"support more than 64 connected links")
	}

	hObj := fnv.New64a()
	if _, err := hObj.Write(key); err != nil {
		return mask, err
	}

	h := hObj.Sum64()
	i := len(repl.links)

	var j uint32
	var realJ int
	subs := make(map[int]int)
	selected := 0
	for i > 0 {
		j = uint32(hash.JumpHash(h, i))
		realJ = int(j)
		for {
			_, ok := subs[realJ]
			if !ok {
				break
			}
			realJ = subs[realJ]
		}
		mask |= (1 << uint(realJ))
		selected++
		if selected >= repl.replFactor {
			break
		}
		h ^= h >> 12
		h ^= h << 25
		h ^= h >> 27
		h *= uint64(2685821657736338717)
		i--
		subs[int(j)] = i
	}

	return mask, nil
}

func (repl *Replicator) linksForKey(key []byte) ([]core.Link, error) {
	if repl.replFactor > len(repl.links) {
		return nil, fmt.Errorf("The number of replicas exceeds the number" +
			" of active nodes")
	}

	linksCp := make([]core.Link, len(repl.links))
	for ix, link := range repl.links {
		linksCp[ix] = link
	}

	hObj := fnv.New64a()
	if _, err := hObj.Write(key); err != nil {
		return nil, err
	}

	h := hObj.Sum64()
	i := len(linksCp)

	res := make([]core.Link, repl.replFactor)
	resIx := 0
	for i > 0 {
		j := hash.JumpHash(h, i)
		res[resIx] = linksCp[j]
		resIx++

		if resIx >= repl.replFactor {
			break
		}

		h ^= h >> 12
		h ^= h << 25
		h ^= h >> 27
		h *= uint64(2685821657736338717)
		i--
		linksCp[j] = linksCp[i]
	}

	return res, nil
}
