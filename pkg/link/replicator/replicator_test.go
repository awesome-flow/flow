package link

import (
	"reflect"
	"testing"

	"github.com/whiteboxio/flow/pkg/core"
)

type TestLink struct {
	Name    string
	lastMsg *core.Message
	*core.Connector
}

func NewTestLink(name string) *TestLink {
	return &TestLink{name, nil, core.NewConnector()}
}

func (tl *TestLink) Recv(msg *core.Message) error {
	tl.lastMsg = msg
	return msg.AckDone()
}

func (tl *TestLink) String() string {
	return tl.Name
}

func TestReplicator_linksIxsForKey(t *testing.T) {
	links := []core.Link{
		NewTestLink("Link1"),
		NewTestLink("Link2"),
		NewTestLink("Link3"),
		NewTestLink("Link4"),
		NewTestLink("Link5"),
		NewTestLink("Link6"),
		NewTestLink("Link7"),
	}

	repl, err := New("replicator", core.Params{"replicas": 3}, core.NewContext())
	if err != nil {
		t.Fatalf("Unexpected error while initializing replicator: %s", err)
	}
	if err := repl.LinkTo(links); err != nil {
		t.Fatalf("Failed to link replicator to links: %s", err)
	}
	tests := []struct {
		key      []byte
		expected uint64
	}{
		{
			key: []byte("msgKey1"),
			//expected: []core.Link{links[2], links[1], links[6]},
			expected: 70, //0b01000110,
		},
		{
			key: []byte("msgKey2"),
			//expected: []core.Link{links[2], links[3], links[0]},
			expected: 13, //0b00001101,
		},
		{
			key: []byte("KeyAno"),
			//expected: []core.Link{links[6], links[2], links[5]},
			expected: 100, //0b01100100,
		},
	}
	for _, tt := range tests {
		links, err := repl.(*Replicator).linksIxsForKey(tt.key)
		if err != nil {
			t.Fatalf("Failed to get the list of links from the replicator: %s", err)
		}
		if !reflect.DeepEqual(links, tt.expected) {
			t.Errorf("Unexpected list of nodes returned by the hashing algorithm:"+
				" %+v, want: %+v", links, tt.expected)
		}
	}
}
