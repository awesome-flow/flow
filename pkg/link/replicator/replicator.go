package link

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/awesome-flow/flow/pkg/core"
	demux "github.com/awesome-flow/flow/pkg/link/demux"
	hash "github.com/awesome-flow/flow/pkg/util/hash"
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
	MsgSendTimeout = 50 * time.Millisecond
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
		core.NewConnectorWithContext(context),
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

	repl.OnSetUp(repl.SetUp)
	repl.OnTearDown(repl.TearDown)

	return repl, nil
}

func (repl *Replicator) SetUp() error {
	for _, ch := range repl.GetMsgCh() {
		go repl.replicate(ch)
	}

	return nil
}

func (repl *Replicator) TearDown() error {
	for _, ch := range repl.GetMsgCh() {
		close(ch)
	}
	return nil
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
	//TODO (olegs)
	panic("Not implemented")
}

func (repl *Replicator) replicate(ch chan *core.Message) {
	var msgKey []byte
	for msg := range ch {
		if repl.hashKey == "" {
			if v := msg.Payload(); len(v) > 128 {
				msgKey = v[:128]
			} else {
				msgKey = v
			}
		} else {
			if v, ok := msg.Meta(repl.hashKey); ok {
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

		linksIxs, err := linksIxsForKey(msgKey, repl.replFactor, len(repl.links))
		if err != nil {
			log.Errorf("Failed to get a list of links for key %s: %s", msgKey, err)
		}

		if err := demux.Demultiplex(msg, linksIxs, repl.links, MsgSendTimeout); err != nil {
			log.Errorf("Replicator failed to send message: %q", err)
		}
	}
}

func linksIxsForKey(key []byte, replFactor, nLinks int) (uint64, error) {
	mask := demux.DemuxMaskNone

	if nLinks > 64 {
		return mask, fmt.Errorf("The current version of replicator does not" +
			" support more than 64 connected links")
	}

	hObj := fnv.New64a()
	if _, err := hObj.Write(key); err != nil {
		return demux.DemuxMaskNone, err
	}

	h := hObj.Sum64()
	i := nLinks

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
		if selected >= replFactor {
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
