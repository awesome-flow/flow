package link

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/whiteboxio/flow/pkg/metrics"

	log "github.com/sirupsen/logrus"

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

	go repl.replicate()

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

		//log.Infof("Sending a message: %s", msg.Payload)

		links, err := repl.linksForKey(msgKey)
		if err != nil {
			log.Errorf("Failed to get a list of links for key %s: %s", msgKey, err)
		}
		wg := &sync.WaitGroup{}
		for _, link := range links {
			wg.Add(1)
			go func(l core.Link) {
				// log.Infof("Routing the message identified by: %s to %s",
				// 	string(msgKey), l.String())
				msgCp := core.CpMessage(msg)
				if sendErr := l.Recv(msgCp); sendErr != nil {
					metrics.GetCounter(
						fmt.Sprintf("link.replicator.%s.msg.failed", link)).Inc(1)
					return
				}
				metrics.GetCounter(
					fmt.Sprintf("link.replicator.%s.msg.sent", link)).Inc(1)

				<-msgCp.GetAckCh()
				wg.Done()
			}(link)
		}
		ack := make(chan bool, 1)
		timeout := false
		go func() {
			wg.Wait()
			if !timeout {
				ack <- true
			}
		}()
		select {
		case <-ack:
			msg.AckDone()
		case <-time.After(ReplMsgSendTimeout):
			timeout = true
			msg.AckTimedOut()
		}
		close(ack)
	}
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
