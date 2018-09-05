package file

import (
	"io/ioutil"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/fsnotify/fsnotify"
)

type VolatileFile struct {
	path       string
	once       *sync.Once
	watcher    *fsnotify.Watcher
	lock       *sync.Mutex
	notifyChan chan bool
}

func New(path string) (*VolatileFile, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	vf := &VolatileFile{
		path:       path,
		once:       &sync.Once{},
		lock:       &sync.Mutex{},
		notifyChan: make(chan bool, 1),
		watcher:    w,
	}

	if err := vf.Deploy(); err != nil {
		return nil, err
	}

	return vf, nil
}

func (vf *VolatileFile) Deploy() error {
	log.Infof("Deploying a watcher for path: %s", vf.path)
	vf.once.Do(func() {
		go func() {
			for event := range vf.watcher.Events {
				log.Infof("Received a new FS event: %s", event.String())
				switch event.Op {
				case fsnotify.Create, fsnotify.Write, fsnotify.Remove:
					vf.notify()
				default:
					log.Infof("Ingoring FS event")
				}
			}
		}()

	})
	return vf.watcher.Add(vf.path)
}

func (vf *VolatileFile) notify() {
	vf.lock.Lock()
	defer vf.lock.Unlock()
	for len(vf.notifyChan) > 0 {
		<-vf.notifyChan
	}
	vf.notifyChan <- true
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
	return vf.InterpretData(rawData)
}

func (vf *VolatileFile) GetPath() string {
	return vf.path
}

func (vf *VolatileFile) GetNotifyChan() chan bool {
	return vf.notifyChan
}

// InterpretData is expected to be overriden by structs embedding VolatileFile
func (vf *VolatileFile) InterpretData(rawData []byte) (interface{}, error) {
	return rawData, nil
}
