package file

import (
	"fmt"
	"io/ioutil"
	"sync"

	log "github.com/sirupsen/logrus"

	event "github.com/awesome-flow/flow/pkg/util/file/event"
	"github.com/fsnotify/fsnotify"
)

const (
	VFPermDefault = 0644
)

type VolatileFile struct {
	path    string
	once    *sync.Once
	watcher *fsnotify.Watcher
	notify  chan *event.Event
}

func New(path string) (*VolatileFile, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	vf := &VolatileFile{
		path:    path,
		once:    &sync.Once{},
		watcher: w,
		notify:  make(chan *event.Event),
	}

	return vf, nil
}

func (vf *VolatileFile) Deploy() error {
	log.Infof("Deploying a watcher for path: %s", vf.path)
	vf.once.Do(func() {
		go func() {
			for ntf := range vf.watcher.Events {
				log.Infof("Received a new fsnotify notification: %s", ntf)
				switch ntf.Op {
				case fsnotify.Create:
					vf.notify <- event.New(event.Create)
				case fsnotify.Write:
					vf.notify <- event.New(event.Update)
				case fsnotify.Remove:
					vf.notify <- event.New(event.Delete)
				default:
					log.Infof("Ignored event: %s", ntf.String())
				}
			}
		}()
		vf.watcher.Add(vf.path)
	})
	return nil
}

func (vf *VolatileFile) TearDown() error {
	log.Infof("Removing the watcher for path: %s", vf.path)
	return vf.watcher.Remove(vf.path)
}

func (vf *VolatileFile) ReadRawData() ([]byte, error) {
	rawData, err := ioutil.ReadFile(vf.path)
	if err != nil {
		return nil, err
	}
	return rawData, nil
}

func (vf *VolatileFile) ReadData() (interface{}, error) {
	return vf.ReadRawData()
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

func (vf *VolatileFile) GetNotifyChan() chan *event.Event {
	return vf.notify
}

func (vf *VolatileFile) EncodeData(data interface{}) ([]byte, error) {
	if byteData, ok := data.([]byte); ok {
		return byteData, nil
	}
	return nil, fmt.Errorf("Failed to convert data to []byte")
}
