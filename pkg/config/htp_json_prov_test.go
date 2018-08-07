package config

import (
	"encoding/json"
	"math"
	"net/http"
	"reflect"
	"testing"
	"time"
)

type testStruct struct {
	Foo string
	Bar uint16
	Baz map[string]bool
}

type testHandler struct {
	data []byte
}

func newTestHandler(data []byte) *testHandler {
	return &testHandler{data}
}

func (th *testHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	rw.WriteHeader(200)
	rw.Write(th.data)
}

func TestHttpJsonProv_Resolve(t *testing.T) {

	hjp, err := RegisterHttpJsonProv("test_json",
		"http://localhost:25000/config.json", &testStruct{}, 0)
	if err != nil {
		t.Fatalf("Failed to register json provider: %s", err.Error())
	}

	ts := &testStruct{
		Foo: "A",
		Bar: math.MaxUint16,
		Baz: map[string]bool{
			"A": true,
			"B": false,
		},
	}

	b, err := json.Marshal(ts)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	srv := &http.Server{
		Addr:         ":25000",
		Handler:      newTestHandler(b),
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			t.Fatalf("Unexpected test http server error: %s", err.Error())
		}
	}()

	time.Sleep(100 * time.Millisecond)
	data, ok := Get("test_json")
	if !ok {
		t.Errorf("Config is missing json value. Last err: %s", hjp.lastErr.Load())
	}

	if !reflect.DeepEqual(data, ts) {
		t.Fatalf("Unexpected data returned by the config: %+v", data)
	}

	srv.Close()
}
