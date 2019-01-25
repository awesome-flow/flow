package devenv

import "testing"

func Test_DockerComposeBuilder(t *testing.T) {
	fragments := []DockerComposeFragment{
		`
  foo:
    bar: baz`,
		`
  abc:
    - def
    - ghk`,
	}

	got, err := DockerComposeBuilder(fragments)
	if err != nil {
		t.Fatal(err)
	}

	expected := `
version: '3'

services:
  foo:
    bar: baz
  abc:
    - def
    - ghk
`

	if expected != got {
		t.Fatalf("Mismatched render: want: %q, got: %q", expected, got)
	}

}
