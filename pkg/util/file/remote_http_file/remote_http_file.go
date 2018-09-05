package file

import (
	"fmt"

	"github.com/fsnotify/fsnotify"
	vf "github.com/whiteboxio/flow/pkg/util/file/volatile_file"
)

type RemoteHttpFile struct {
	*vf.VolatileFile
}

func New(path string) (*RemoteHttpFile, error) {
	vol, err := vf.New(path)
	if err != nil {
		return nil, err
	}
	rhf := &RemoteHttpFile{vol}
	return rhf, nil
}

func (rhf *RemoteHttpFile) Deploy() error {
	//TODO
	return nil
}

func (rhf *RemoteHttpFile) TearDown() error {
	//TODO
	return nil
}

func (rhf *RemoteHttpFile) ReadData() (interface{}, error) {
	//TODO
	return nil, nil
}

func (rhf *RemoteHttpFile) WrieData(data interface{}) error {
	return fmt.Errorf("Remote HTTP file is read-only")
}

func (rfh *RemoteHttpFile) GetNotifyChan() chan fsnotify.Event {
	//TODO
	return nil
}
