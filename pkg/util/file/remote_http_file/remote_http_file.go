package file

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/cenk/backoff"
	log "github.com/sirupsen/logrus"

	event "github.com/awesome-flow/flow/pkg/util/file/event"
	vf "github.com/awesome-flow/flow/pkg/util/file/volatile_file"
)

type RemoteHttpFile struct {
	ttl         time.Duration
	fetchedData []byte
	lastErr     error
	lastMod     string
	deployOnce  *sync.Once
	fetchOnce   *sync.Once
	shouldStop  bool
	*vf.VolatileFile
}

var DefaultRequestTimeout = 5 * time.Second

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
		nil,
		nil,
		"",
		&sync.Once{},
		&sync.Once{},
		false,
		vol,
	}
	return rhf, nil
}

func (rhf *RemoteHttpFile) Deploy() error {
	rhf.deployOnce.Do(func() {
		go func() {
			ticker := backoff.NewTicker(backoff.NewConstantBackOff(rhf.ttl))
			fetchOrReport := func() {
				if err := rhf.DoFetch(); err != nil {
					rhf.lastErr = err
					log.Errorf("Failed to fetch remote http file: %s", err)
				}
			}
			rhf.fetchOnce.Do(fetchOrReport)
			for _ = range ticker.C {
				fetchOrReport()
				if rhf.shouldStop {
					ticker.Stop()
				}
			}
		}()
	})

	return nil
}

func (rhf *RemoteHttpFile) TearDown() error {
	rhf.shouldStop = true
	close(rhf.GetNotifyChan())
	return nil
}

func (rhf *RemoteHttpFile) DoFetch() error {
	client := http.Client{
		Timeout: DefaultRequestTimeout,
	}
	resp, err := client.Get(rhf.GetPath())
	if err != nil {
		rhf.lastErr = err
		return err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		rhf.lastErr = err
		return err
	}

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotModified:
		log.Infof("Got server not modified branch")
		if rhf.fetchedData == nil {
			rhf.lastErr = fmt.Errorf("Server returned http.StatusNotModified" +
				" but there is no previous result yet")
			return rhf.lastErr
		}
	default:
		rhf.lastErr = fmt.Errorf("Bad response status: %d. Reason: %s",
			resp.StatusCode, body)
		return rhf.lastErr
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
		// To prevent the update event from triggering on cold start and
		// dead-locking the channel
		if rhf.fetchedData == nil {
			rhf.fetchedData = body
		}
		if bytes.Compare(rhf.fetchedData, body) != 0 && len(body) != 0 {
			log.Info("Received an updated response")
			rhf.fetchedData = body
			log.Infof("Remote time: %s", tRemote)

			log.Info("Sending a new notification")
			for len(rhf.GetNotifyChan()) > 0 {
				<-rhf.GetNotifyChan()
			}
			log.Infof("Notification channel length: %d", len(rhf.GetNotifyChan()))
			rhf.GetNotifyChan() <- event.New(event.Update)
			log.Info("Sent a notification")
		} else {
			log.Infof("No effective change detected")
		}
	} else {
		log.Infof("No changes detected since the recent update")
	}
	return nil
}

func (rhf *RemoteHttpFile) ReadRawData() ([]byte, error) {
	rhf.fetchOnce.Do(func() { rhf.DoFetch() })
	return rhf.fetchedData, rhf.lastErr
}

func (rhf *RemoteHttpFile) ReadData() (interface{}, error) {
	return rhf.ReadRawData()
}

func (rhf *RemoteHttpFile) WrieData(data interface{}) error {
	return fmt.Errorf("Remote HTTP file is read-only")
}
