package pipeline

import (
	"testing"
)

func TestDotExplainer_Explain(t *testing.T) {

	tests := []struct {
		name     string
		body     string
		expected string
	}{
		{
			name: "Single element",
			body: `
pipeline:
  A:
`,
			expected: "digraph G{\nA\n}",
		},
		{
			name: "Single connection",
			body: `
pipeline:
  A:
    connect: B
`,
			expected: "digraph G{\nA -> B\n}",
		},
		{
			name: "Chain of elements",
			body: `
pipeline:
  A:
    connect: B
  B:
    connect: C
`,
			expected: "digraph G{\nA -> B\nB -> C\n}",
		},
		{
			name: "Node with outcoming links",
			body: `
pipeline:
  A:
    links:
      - B
      - C
      - D
`,
			expected: "digraph G{\n\n      A -> B\n      A -> C\n      A -> D\n}",
		},
		{
			name: "Node with incoming links",
			body: `
pipeline:			
  A:
    links:
      - B
      - C
    connect: D
`,
			expected: "digraph G{\nA -> D\n      B -> A\n      C -> A\n}",
		},
		{
			name: "Node with routes",
			body: `
pipeline:
  A:
    routes:
      foo: B
      bar: C
      baz: D
`,
			expected: "digraph G{\nA -> C [label=\"bar\"]\n    A -> D [label=\"baz\"]\n    A -> B [label=\"foo\"]\n    \n}",
		},
	}

	t.Parallel()

	for _, testcase := range tests {
		t.Run(testcase.name, func(t *testing.T) {
			cfg, err := cfgFromYaml([]byte(testcase.body))
			if err != nil {
				t.Fatalf("Failed to parse yaml config: %s", err)
			}

			pipeline := &Pipeline{
				compsCfg: cfg.Actors,
				pplCfg:   cfg.Pipeline,
			}

			dotexplain := &DotExplainer{}
			explained, err := dotexplain.Explain(pipeline)
			if err != nil {
				t.Fatalf("Failed to explain the pipeline: %s", err)
			}

			if explained != testcase.expected {
				t.Fatalf("Unexpected explanation: %q", explained)
			}
		})
	}
}
