package config

import (
	"bytes"
	"io/ioutil"
	"os"
	"reflect"
	"regexp"
	"testing"
	"time"
)

func TestCacheFile_New(t *testing.T) {
	path := "test_file"
	ttl := 1 * time.Second
	f, err := New(path, ttl)
	if err != nil {
		t.Fatalf("Failed to instantiate a new cache file: %s", err)
	}
	expStruct := &CacheFile{path: path, ttl: ttl}
	if !reflect.DeepEqual(f, expStruct) {
		t.Fatalf("Unexpected CacheFile structure contents: %+v. Want: %+v",
			f, expStruct)
	}
}

func TestCacheFile_Read(t *testing.T) {

	tests := []struct {
		name       string
		ttl        time.Duration
		sleep      time.Duration
		payload    []byte
		expPayload []byte
		expErrMsg  string
	}{
		{
			"Valid file",
			time.Hour,
			0 * time.Second,
			[]byte("foo:bar"),
			[]byte("foo:bar"),
			"",
		},
		{
			"Expired file",
			time.Microsecond,
			10 * time.Millisecond,
			[]byte("foo:bar"),
			[]byte{},
			"^File\\s.*\\shas\\sexpired.*",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			tmpfile, err := ioutil.TempFile("/tmp", "flow-test-cache-file")
			if err != nil {
				t.Fatalf("Failed to create a new tmp file: %s", err)
			}
			defer os.Remove(tmpfile.Name())

			if writeErr := ioutil.WriteFile(tmpfile.Name(), testCase.payload, 0644); writeErr != nil {
				t.Fatalf("Failed to write to tmp file: %s", writeErr)
			}

			time.Sleep(testCase.sleep)

			f, err := New(tmpfile.Name(), testCase.ttl)
			if err != nil {
				t.Fatalf("Failed to instantiate a new CacheFile: %s", err)
			}

			readData, err := f.Read()

			if testCase.expErrMsg != "" {
				if err == nil {
					t.Fatalf("Expected to get an error like: %s, got nil",
						testCase.expErrMsg)
				}

				errMsg := err.Error()
				match, err := regexp.Match(testCase.expErrMsg, []byte(errMsg))
				if err != nil {
					t.Fatalf("Failed to compile a regex: %s", err)
				}
				if !match {
					t.Fatalf("Wrong assert on error match: got: %s, want: %s",
						errMsg, testCase.expErrMsg)
				}
			} else {
				if err != nil {
					t.Fatalf("Failed to read cache file: %s", err)
				}

				if bytes.Compare(readData, testCase.expPayload) != 0 {
					t.Fatalf("Unexpected contents of the file: [%s]. Want: [%s]",
						readData, testCase.expPayload)
				}
			}
		})
	}

}
