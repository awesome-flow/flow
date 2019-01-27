package config

import (
	"reflect"
	"testing"

	testutils "github.com/awesome-flow/flow/pkg/util/test"
)

func Test_SimpleProv(t *testing.T) {
	key := string(testutils.RandStringBytes(128))
	value := testutils.RandStringBytes(4096)

	sp := NewSimpleProv(key, value)
	if err := sp.Setup(); err != nil {
		t.Fatalf(err.Error())
	}

	gotval, ok := Get(key)
	if !ok {
		t.Fatal("Could not get value form the registry")
	}

	if !reflect.DeepEqual(gotval, value) {
		t.Fatalf("Unexpected value returned from the registry: %q, want: %q", gotval, value)
	}
}
