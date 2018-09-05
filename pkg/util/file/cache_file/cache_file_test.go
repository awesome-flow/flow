package file

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
				match, err := regexp.MatchString(testCase.expErrMsg, errMsg)
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

func TestCacheFile_IsValid(t *testing.T) {
	tests := []struct {
		name      string
		payload   []byte
		ttl       time.Duration
		sleep     time.Duration
		expected  bool
		reasonWhy string
	}{
		{
			"Valid file",
			[]byte("foo:bar"),
			time.Hour,
			0 * time.Second,
			true,
			"",
		},
		{
			"Expired file",
			[]byte("foo:bar"),
			20 * time.Millisecond,
			30 * time.Millisecond,
			false,
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
				t.Fatalf("Failed to instantiate a new CacheFile: %s",
					err)
			}

			valid, why := f.IsValid()

			if testCase.expected != valid {
				t.Fatalf("Unexpected validity check result: %t, want: %t. "+
					"Reason message: %s", valid, testCase.expected, why)
			}

			if testCase.reasonWhy != "" {
				match, err := regexp.MatchString(testCase.reasonWhy, why.Error())
				if err != nil {
					t.Fatalf("Failed to compile a regex: %s", err)
				}
				if !match {
					t.Fatalf("Wrong assert on error match: got: %s, want: %s",
						why.Error(), testCase.reasonWhy)
				}
			}
		})
	}
}

func TestCacheFile_Consolidate(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		errMsg  string
	}{
		{
			name:    "Regular file",
			payload: []byte("foo:bar"),
			errMsg:  "",
		},
		{
			name:    "Empty file",
			payload: []byte{},
			errMsg:  "^Data is empty.*",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			tmpFile, err := ioutil.TempFile(TmpFileFolder, TmpFilePreffix)
			if err != nil {
				t.Fatalf("Failed to create a new tmp file: %s", err)
			}
			defer os.Remove(tmpFile.Name())
			f, err := New(tmpFile.Name(), time.Hour)
			if err != nil {
				t.Fatalf("Failed to instantiate a new CacheFile: %s", err)
			}
			if writeErr := f.Consolidate(testCase.payload); writeErr != nil {
				if testCase.errMsg != "" {
					match, err := regexp.MatchString(testCase.errMsg, writeErr.Error())
					if err != nil {
						t.Fatalf("Failed to match against regex: %s", err)
					}
					if !match {
						t.Fatalf("Failed to assert error message similarity: got %s, want %s",
							writeErr.Error(), testCase.errMsg)
					}
				} else {
					t.Fatalf("Failed to consolidate data: %s", writeErr)
				}
			}

			data, err := ioutil.ReadFile(tmpFile.Name())
			if err != nil {
				t.Fatalf("Failed to read data from the file: %s", err)
			}
			if bytes.Compare(data, testCase.payload) != 0 {
				t.Fatalf("The payload doesn't match: got %s, want %s",
					data, testCase.payload)
			}
		})
	}
}

func TestCacheFile_Invalidate(t *testing.T) {
	tests := []struct {
		name string
		ttl  time.Duration
	}{
		{
			"Regular file",
			time.Hour,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			tmpFile, err := ioutil.TempFile(TmpFileFolder, TmpFilePreffix)
			if err != nil {
				t.Fatalf("Failed to create a tmp file: %s", err)
			}
			defer os.Remove(tmpFile.Name())
			f, err := New(tmpFile.Name(), testCase.ttl)
			if err != nil {
				t.Fatalf("Failed to instantiate a new CacheFile: %s", err)
			}
			if valid, _ := f.IsValid(); !valid {
				t.Fatalf("Expected the file to be valid, returned invalid")
			}
			if rmErr := f.Invalidate(); rmErr != nil {
				t.Fatalf("Failed to invalidate file: %s", rmErr)
			}
			_, statErr := os.Stat(tmpFile.Name())
			if statErr == nil {
				t.Fatalf("Invalidate was supposed to remove the file but stat " +
					"returned no error")
			}
			if !os.IsNotExist(statErr) {
				t.Fatalf("Unexpected error on stat call: %s", statErr)
			}
		})
	}
}
