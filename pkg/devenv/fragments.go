package devenv

import (
	"bytes"
	"text/template"
)

type Fragment interface{}

type DockerComposeFragment string

func DockerComposeBuilder(fragments []DockerComposeFragment) (string, error) {
	dockercompose, err := template.New("docker-compose").Parse(`version: '3'

services:
{{- range .Fragments}}
{{- .}}
{{- end}}
`)
	if err != nil {
		return "", err
	}

	data := struct{ Fragments []DockerComposeFragment }{
		Fragments: fragments,
	}

	var buf bytes.Buffer
	if err := dockercompose.Execute(&buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}
