package file

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/fsnotify/fsnotify"
	vf "github.com/whiteboxio/flow/pkg/util/file/volatile_file"
)

type RemoteHttpFile struct {
	ttl         time.Duration
	shouldStop  bool
	fetchedData []byte
	lastErr     error
	lastMod     string
	once        *sync.Once
	deployOnce  *sync.Once
	*vf.VolatileFile
}

func New(path string) (*RemoteHttpFile, error) {
	return NewWithInterval(path, time.Minute)
}

func NewWithInterval(path string, ttl time.Duration) (*RemoteHttpFile, error) {
	vol, err := vf.New(path)
	if err != nil {
		return nil, err
	}
	rhf := &RemoteHttpFile{
		ttl,
		false,
		nil,
		nil,
		"",
		&sync.Once{},
		&sync.Once{},
		vol,
	}
	return rhf, nil
}

func (rhf *RemoteHttpFile) Deploy() error {
	rhf.deployOnce.Do(func() {
		rhf.once.Do(rhf.DoFetch)
		go func() {
			for {
				time.Sleep(rhf.ttl)
				rhf.DoFetch()
			}
		}()
	})

	return nil
}

func (rhf *RemoteHttpFile) TearDown() error {
	//TODO
	return nil
}

func (rhf *RemoteHttpFile) DoFetch() {

	resp, err := http.Get(rhf.GetPath())
	if err != nil {
		rhf.lastErr = err
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK ||
		resp.StatusCode >= http.StatusMultipleChoices {
		rhf.lastErr = fmt.Errorf("Bad response status: %d", resp.StatusCode)
	}
	rhf.lastErr = nil
	lms := resp.Header.Get("Last-Modified")
	var tRemote, tLocal time.Time
	if rhf.lastMod != "" {
		tLocal, err = http.ParseTime(rhf.lastMod)
		if err != nil {
			log.Warnf("Failed to parse local Last-Modified header [%s]: %s",
				lms, err)
		}
	}
	if lms != "" {
		tRemote, err = http.ParseTime(lms)
		if err != nil {
			log.Warnf("Failed to parse remote Last-Modified header [%s]: %s",
				lms, err)
		}
		rhf.lastMod = lms
	}
	if tRemote.IsZero() || tRemote.After(tLocal) {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Warnf("Failed to read response body: %s", err)
		}
		if bytes.Compare(rhf.fetchedData, body) != 0 {
			log.Info("Received an updated response")
			rhf.fetchedData = body
			rhf.GetNotifyChan() <- fsnotify.Event{Op: fsnotify.Write}
		} else {
			log.Infof("No effective change detected")
		}
	} else {
		log.Infof("No changes detected since the recent update")
	}
	return
}

func (rhf *RemoteHttpFile) ReadData() (interface{}, error) {
	rhf.once.Do(rhf.DoFetch)
	return rhf.fetchedData, rhf.lastErr
}

func (rhf *RemoteHttpFile) WrieData(data interface{}) error {
	return fmt.Errorf("Remote HTTP file is read-only")
}

func (rfh *RemoteHttpFile) GetNotifyChan() chan fsnotify.Event {
	//TODO
	return nil
}
