package x

import (
	"reflect"
	"testing"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	coretest "github.com/awesome-flow/flow/pkg/corev1alpha1/test"
)

func TestMetaParserParseMeta(t *testing.T) {
	tests := []struct {
		name    string
		body    []byte
		expmeta map[string]interface{}
		expbody []byte
		experr  error
	}{
		{
			name:    "plain message",
			body:    []byte("foo"),
			expmeta: map[string]interface{}{},
			expbody: []byte("foo"),
		},
		{
			name: "message with correct meta",
			body: []byte("foo=bar&boo=baz hello-world"),
			expmeta: map[string]interface{}{
				"foo": "bar",
				"boo": "baz",
			},
			expbody: []byte("hello-world"),
		},
		{
			name: "message with dummy meta",
			body: []byte("foo bar"),
			expmeta: map[string]interface{}{
				"foo": "",
			},
			expbody: []byte("bar"),
		},
		{
			name: "message with multiple space runes",
			body: []byte("foo bar baz"),
			expmeta: map[string]interface{}{
				"foo": "",
			},
			expbody: []byte("bar baz"),
		},
		{
			name: "message and meta separated with double space rune",
			body: []byte("foo  bar"),
			expmeta: map[string]interface{}{
				"foo": "",
			},
			expbody: []byte(" bar"),
		},
		{
			name:    "empty message",
			body:    []byte{},
			expmeta: map[string]interface{}{},
			expbody: []byte{},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			msg := core.NewMessage(testCase.body)
			parsed, err := parseMsgMeta(msg)
			if !coretest.EqErr(err, testCase.experr) {
				t.Fatalf("unexpected parse error: got: %s, want: %s", err, testCase.experr)
			}
			if testCase.experr != nil {
				return
			}
			if !reflect.DeepEqual(parsed.Body(), testCase.expbody) {
				t.Fatalf("unexpected message body: got: %q, want: %q", parsed.Body(), testCase.expbody)
			}
			for _, k := range parsed.MetaKeys() {
				v, _ := parsed.Meta(k)
				if v.(string) != testCase.expmeta[k.(string)] {
					t.Fatalf("unexpected message meta for key %q: got: %q, want: %q", k, v.(string), testCase.expmeta[k.(string)])
				}
				delete(testCase.expmeta, k.(string))
			}
			if len(testCase.expmeta) > 0 {
				t.Fatalf("missing meta keys: %+v", testCase.expmeta)
			}
		})
	}
}
