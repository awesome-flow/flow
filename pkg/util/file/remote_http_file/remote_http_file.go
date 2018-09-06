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
	shouldStop  bool
	fetchedData []byte
	lastErr     error
	lastMod     string
	lock        sync.Mutex
	*vf.VolatileFile
}

func New(path string) (*RemoteHttpFile, error) {
	vol, err := vf.New(path)
	if err != nil {
		return nil, err
	}
	rhf := &RemoteHttpFile{
		false,
		nil,
		nil,
		"",
		sync.Mutex{},
		vol,
	}
	return rhf, nil
}

func (rhf *RemoteHttpFile) Deploy() error {

	return nil
}

func (rhf *RemoteHttpFile) TearDown() error {
	//TODO
	return nil
}

func (rhf *RemoteHttpFile) DoFetch() {
	rhf.lock.Lock()
	defer rhf.lock.Unlock()

	resp, err := http.Get(rhf.GetPath())
	defer resp.Body.Close()

	if err != nil {
		rhf.lastErr = err
		return
	}

	if resp.StatusCode < http.StatusOK ||
		resp.StatusCode >= http.StatusMultipleChoices {
		rhf.lastErr = fmt.Errorf("Bad response status: %d", resp.StatusCode)
	}
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
		return
	} else {
		log.Infof("No changes detected since the recent update")
	}
}

func (rhf *RemoteHttpFile) ReadData() (interface{}, error) {

	return nil, nil
}

func (rhf *RemoteHttpFile) WrieData(data interface{}) error {
	return fmt.Errorf("Remote HTTP file is read-only")
}

func (rfh *RemoteHttpFile) GetNotifyChan() chan fsnotify.Event {
	//TODO
	return nil
}
