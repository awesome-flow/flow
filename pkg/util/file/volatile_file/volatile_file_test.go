package file

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	file_event "github.com/awesome-flow/flow/pkg/util/file/event"
)

func TestVolatileFile_Deploy(t *testing.T) {
	tmpFile, err := ioutil.TempFile("/tmp", "flow-test-volatile-file")
	if err != nil {
		t.Fatalf("Failed to create a tmp file: %s", err)
	}
	defer os.Remove(tmpFile.Name())
	vf, err := New(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to instantiate a new volatile file: %s", err)
	}
	if err := vf.Deploy(); err != nil {
		t.Fatalf("Failed to deploy volatile file watcher: %s", err)
	}
	eventChan := vf.GetNotifyChan()
	data := []byte("foo:bar")
	if err := ioutil.WriteFile(tmpFile.Name(), data, 0444); err != nil {
		t.Fatalf("Failed to write data to tmp file: %s", err)
	}
	select {
	case event := <-eventChan:
		if event.Type != file_event.Update {
			t.Fatalf("Unexpected file event: %s, want: Write", event.String())
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Timed out to receive a FS notification")
	}
	if err := vf.TearDown(); err != nil {
		t.Fatalf("Failed to TearDown volatile file: %s", err)
	}
}

func TestVolatileFile_ReadWriteData(t *testing.T) {
	data := []byte("foo:bar")
	tmpFile, err := ioutil.TempFile("/tmp", "flow-test-volatile-file")
	if err != nil {
		t.Fatalf("Failed to create a tmp file: %s", err)
	}
	defer os.Remove(tmpFile.Name())
	vf, err := New(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to instantiate a new VolatileFile: %s", err)
	}
	if err := vf.WriteData(data); err != nil {
		t.Fatalf("Failed to write data: %s", err)
	}
	readData, err := vf.ReadData()
	if err != nil {
		t.Fatalf("Failed to read data: %s", err)
	}
	if bytes.Compare(data, readData.([]byte)) != 0 {
		t.Fatalf("Expected and factual data diverges: %s Vs %s", data, readData)
	}
}
