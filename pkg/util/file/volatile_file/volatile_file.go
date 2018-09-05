package file

import (
	"fmt"
	"io/ioutil"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/fsnotify/fsnotify"
)

const (
	VFPermDefault = 0644
)

type VolatileFile struct {
	path    string
	once    *sync.Once
	watcher *fsnotify.Watcher
	lock    *sync.Mutex
}

func New(path string) (*VolatileFile, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	vf := &VolatileFile{
		path:    path,
		once:    &sync.Once{},
		lock:    &sync.Mutex{},
		watcher: w,
	}

	if err := vf.Deploy(); err != nil {
		return nil, err
	}

	return vf, nil
}

func (vf *VolatileFile) Deploy() error {
	log.Infof("Deploying a watcher for path: %s", vf.path)
	return vf.watcher.Add(vf.path)
}

func (vf *VolatileFile) TearDown() error {
	log.Infof("Removing the watcher for path: %s", vf.path)
	return vf.watcher.Remove(vf.path)
}

func (vf *VolatileFile) ReadData() (interface{}, error) {
	rawData, err := ioutil.ReadFile(vf.path)
	if err != nil {
		return nil, err
	}
	return vf.DecodeData(rawData)
}

func (vf *VolatileFile) WriteData(data interface{}) error {
	rawData, err := vf.EncodeData(data)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(vf.path, rawData, VFPermDefault)
}

func (vf *VolatileFile) GetPath() string {
	return vf.path
}

func (vf *VolatileFile) GetNotifyChan() chan fsnotify.Event {
	return vf.watcher.Events
}

func (vf *VolatileFile) DecodeData(rawData []byte) (interface{}, error) {
	return rawData, nil
}

func (vf *VolatileFile) EncodeData(data interface{}) ([]byte, error) {
	if byteData, ok := data.([]byte); ok {
		return byteData, nil
	}
	return nil, fmt.Errorf("Failed to convert data to []byte")
}
