package sink

import (
	"fmt"
	"strings"
	"testing"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/devenv"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

func Test_UDP_DevEnv(t *testing.T) {
	port := testutil.RandInt(32000)
	udp, err := New(
		"udp",
		core.Params{"bind_addr": fmt.Sprintf(":%d", port)},
		core.NewContext(),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &devenv.Context{}
	fragments, err := udp.DevEnv(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var dcf devenv.DockerComposeFragment
	var ok bool
	for _, fragment := range fragments {
		if dcf, ok = fragment.(devenv.DockerComposeFragment); ok {
			break
		}
	}
	if !ok {
		t.Fatal("No docker compose elements returned by DevEnv")
	}

	if strings.Index(string(dcf), fmt.Sprintf("UDP_SERVER_PORT: %d", port)) == -1 {
		t.Fatalf("Could not find the key substring in docker-compose fragment: %s", dcf)
	}
}
