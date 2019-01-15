package link

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
	testutils "github.com/awesome-flow/flow/pkg/util/test"
)

func TestFanout_RingSize(t *testing.T) {
	ft, ftErr := New("fanout", core.Params{}, core.NewContext())
	if ftErr != nil {
		t.Errorf("Failed to initialize new fanout: %s", ftErr.Error())
	}
	a1, a2, a3 := testutils.NewCountAndReply("car", testutils.ReplyDone), testutils.NewCountAndReply("car", testutils.ReplyDone), testutils.NewCountAndReply("car", testutils.ReplyDone)
	if linkErr := ft.LinkTo([]core.Link{a1, a2, a3}); linkErr != nil {
		t.Errorf("Failed to link fanout: %s", linkErr.Error())
	}
	ringSize := ft.(*Fanout).RingSize()
	if ringSize != 3 {
		t.Errorf("Unexpected ring size: %d", ringSize)
	}
}

func TestFanout_Send(t *testing.T) {
	ft, ftErr := New("fanout", core.Params{}, core.NewContext())
	if ftErr != nil {
		t.Errorf("Failed to initialize new fanout: %s", ftErr.Error())
	}
	a1, a2, a3 := testutils.NewCountAndReply("car", testutils.ReplyDone), testutils.NewCountAndReply("car", testutils.ReplyDone), testutils.NewCountAndReply("car", testutils.ReplyDone)
	if linkErr := ft.LinkTo([]core.Link{a1, a2, a3}); linkErr != nil {
		t.Errorf("Failed to link fanout: %s", linkErr.Error())
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		msg := core.NewMessage([]byte{})
		go func() {
			<-msg.GetAckCh()
			wg.Done()
		}()
		ft.Send(msg)
	}
	wg.Wait()
	if a1.RcvCnt() != 1 && a2.RcvCnt() != 1 && a3.RcvCnt() != 1 {
		t.Errorf("Unexpected rcv counters: %d, %d, %d", a1.RcvCnt(), a2.RcvCnt(), a3.RcvCnt())
	}
}

func TestFanout_Recv(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	ft, ftErr := New("fanout", core.Params{}, core.NewContext())
	if ftErr != nil {
		t.Errorf("Failed to initialize new fanout: %s", ftErr.Error())
	}
	a1, a2, a3 := testutils.NewCountAndReply("car", testutils.ReplyDone), testutils.NewCountAndReply("car", testutils.ReplyDone), testutils.NewCountAndReply("car", testutils.ReplyDone)
	if linkErr := ft.LinkTo([]core.Link{a1, a2, a3}); linkErr != nil {
		t.Errorf("Failed to link fanout: %s", linkErr.Error())
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		msg := core.NewMessage([]byte{})
		go func() {
			<-msg.GetAckCh()
			wg.Done()
		}()
		ft.Send(msg)
	}
	wg.Wait()
	if a1.RcvCnt() != 1 && a2.RcvCnt() != 1 && a3.RcvCnt() != 1 {
		t.Errorf("Unexpected rcv counters: %d, %d, %d", a1.RcvCnt(), a2.RcvCnt(), a3.RcvCnt())
	}
}

func TestFanout_addRingLinkAsHead(t *testing.T) {
	ft, ftErr := New("fanout", core.Params{}, core.NewContext())
	if ftErr != nil {
		t.Fatalf("Failed to initialize a new fanout: %s", ftErr.Error())
	}
	link := testutils.NewCountAndReply("car", testutils.ReplyDone)
	rl := &RingLink{self: link}
	ft.(*Fanout).addRingLink(rl)
	ringSize := ft.(*Fanout).RingSize()
	if ringSize != 1 {
		t.Fatalf("Unexpected ring size: %d", ringSize)
	}
	if ft.(*Fanout).ringHead != rl {
		t.Fatal("Unexpected ring head")
	}
}

func TestFanout_addRingLinkAsExtra(t *testing.T) {
	ft, ftErr := New("fanout", core.Params{}, core.NewContext())
	if ftErr != nil {
		t.Fatalf("Failed to initialize a new fanout: %s", ftErr.Error())
	}
	ft.(*Fanout).addRingLink(&RingLink{self: testutils.NewCountAndReply("car", testutils.ReplyDone)})
	link := testutils.NewCountAndReply("car", testutils.ReplyDone)
	rl := &RingLink{self: link}
	ft.(*Fanout).addRingLink(rl)

	ringSize := ft.(*Fanout).RingSize()
	if ringSize != 2 {
		t.Fatalf("Unexpected ring size: %d", ringSize)
	}
	if ft.(*Fanout).ringHead != rl {
		t.Fatal("Unexpected ring head")
	}
}

func TestFanout_removeRingLinkAsHead(t *testing.T) {
	ft, ftErr := New("fanout", core.Params{}, core.NewContext())
	if ftErr != nil {
		t.Fatalf("Failed to initialize a new fanout: %s", ftErr.Error())
	}
	ft.(*Fanout).addRingLink(&RingLink{self: testutils.NewCountAndReply("car", testutils.ReplyDone)})
	link := &RingLink{self: testutils.NewCountAndReply("car", testutils.ReplyDone)}
	ft.(*Fanout).addRingLink(link)
	if ft.(*Fanout).ringHead != link {
		t.Fatal("Unexpected ring head")
	}
	if rmErr := ft.(*Fanout).removeRingLink(link); rmErr != nil {
		t.Fatalf("Unexpected remove error: %s", rmErr.Error())
	}
	ringSize := ft.(*Fanout).RingSize()
	if ringSize != 1 {
		t.Fatalf("Unexpected reduced ring size: %d", ringSize)
	}
}

func TestFanout_Connections(t *testing.T) {
	ft, ftErr := New("fanout", core.Params{}, core.NewContext())
	if ftErr != nil {
		t.Fatalf("Failed to initialize a new fanout: %s", ftErr.Error())
	}
	a, b, c := testutils.NewCountAndReply("car", testutils.ReplyDone), testutils.NewCountAndReply("car", testutils.ReplyDone), testutils.NewCountAndReply("car", testutils.ReplyDone)
	lA, lB, lC := &RingLink{self: a},
		&RingLink{self: b},
		&RingLink{self: c}
	type linkChecker struct {
		self *RingLink
		next *RingLink
		prev *RingLink
	}

	// The order of these tests is important as it defines the sequence of
	// states the ft instance is being transitioned through.
	tests := []struct {
		name   string
		add    *RingLink
		remove *RingLink
		links  []linkChecker
	}{
		{
			name: "disconnected",
			links: []linkChecker{
				{lA, nil, nil},
				{lB, nil, nil},
				{lC, nil, nil},
			},
		},
		{
			name: "single node",
			add:  lA,
			links: []linkChecker{
				{lA, lA, lA},
				{lB, nil, nil},
				{lC, nil, nil},
			},
		},
		{
			name: "dual nodes",
			add:  lB,
			links: []linkChecker{
				{lA, lB, lB},
				{lB, lA, lA},
				{lC, nil, nil},
			},
		},
		{
			name: "trisom nodes",
			add:  lC,
			links: []linkChecker{
				{lA, lB, lC},
				{lB, lC, lA},
				{lC, lA, lB},
			},
		},
		{
			name:   "remove one node",
			remove: lA,
			links: []linkChecker{
				{lA, nil, nil},
				{lB, lC, lC},
				{lC, lB, lB},
			},
		},
		{
			name:   "down to single node",
			remove: lB,
			links: []linkChecker{
				{lA, nil, nil},
				{lB, nil, nil},
				{lC, lC, lC},
			},
		},
		{
			name:   "remove last node",
			remove: lC,
			links: []linkChecker{
				{lA, nil, nil},
				{lB, nil, nil},
				{lC, nil, nil},
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.add != nil {
				if err := ft.(*Fanout).addRingLink(testCase.add); err != nil {
					t.Errorf("Failed to add link: %s", err.Error())
				}
			}
			if testCase.remove != nil {
				if err := ft.(*Fanout).removeRingLink(testCase.remove); err != nil {
					t.Errorf("Failed to remove link: %s", err.Error())
				}
			}
			for _, lc := range testCase.links {
				self, next, prev := lc.self, lc.next, lc.prev
				if self.next != next {
					t.Errorf("Wrong next link: %+v, got: %+v, want: %+v",
						self, self.next, next)
				}
				if self.prev != prev {
					t.Errorf("Wrong prev link: %+v, got: %+v, want: %+v",
						self, self.prev, prev)
				}
			}
		})
	}
}

func TestFanout_FindLink(t *testing.T) {
	ft, ftErr := New("fanout", core.Params{}, core.NewContext())
	if ftErr != nil {
		t.Fatalf("Failed to initialize fanout: %s", ftErr.Error())
	}
	a, b, c := testutils.NewCountAndReply("car", testutils.ReplyDone), testutils.NewCountAndReply("car", testutils.ReplyDone), testutils.NewCountAndReply("car", testutils.ReplyDone)
	lA, lB, lC := &RingLink{self: a},
		&RingLink{self: b},
		&RingLink{self: c}
	type lookupCheck struct {
		lookup   core.Link
		expected *RingLink
		expBool  bool
	}
	tests := []struct {
		name   string
		add    *RingLink
		remove *RingLink
		checks []*lookupCheck
	}{
		{name: "empty", checks: []*lookupCheck{{a, nil, false}, {b, nil, false}, {c, nil, false}}},
		{name: "add, 1 node", add: lA, checks: []*lookupCheck{{a, lA, true}, {b, nil, false}, {c, nil, false}}},
		{name: "add, 2 nodes", add: lB, checks: []*lookupCheck{{a, lA, true}, {b, lB, true}, {c, nil, false}}},
		{name: "add, 3 nodes", add: lC, checks: []*lookupCheck{{a, lA, true}, {b, lB, true}, {c, lC, true}}},
		{name: "remove, 2 nodes", remove: lA, checks: []*lookupCheck{{a, nil, false}, {b, lB, true}, {c, lC, true}}},
		{name: "remove, 1 node", remove: lB, checks: []*lookupCheck{{a, nil, false}, {b, nil, false}, {c, lC, true}}},
		{name: "remove, empty", remove: lC, checks: []*lookupCheck{{a, nil, false}, {b, nil, false}, {c, nil, false}}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.add != nil {
				if err := ft.(*Fanout).addRingLink(testCase.add); err != nil {
					t.Errorf("Unable to add link: %s", err.Error())
				}
			}
			if testCase.remove != nil {
				if err := ft.(*Fanout).removeRingLink(testCase.remove); err != nil {
					t.Errorf("Unable to remove link: %s", err.Error())
				}
			}
			for _, check := range testCase.checks {
				v, ok := ft.(*Fanout).FindLink(check.lookup)
				if v != check.expected || ok != check.expBool {
					t.Errorf("Lookup returned unexpected results: %+v %t, want: %+v %t",
						v, ok, check.expected, check.expBool)
				}
			}
		})
	}
}
