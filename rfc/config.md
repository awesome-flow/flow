# RFC#1 Configuration layer

Author: Oleg Sidorov (@icanhazbroccoli)
Date: 2019-05-01

## Abstract

Flow translates a pipeline definition into an acting sidecar. The set of
components, their parameters and interaction between them is defined by the
config. In the simplest case it's a yaml file. Apart from yaml, flow gets
settings from command line arguments and evnironment variables. In the current
implementation, command line args and anvironment variables only partially cover
settings coming from the yaml file. On the other hand, yaml config doesn't
tolerate extra configs (for example, plugin.path does not belong there and would
fire up a parsing error). This approach is a massive blocker for new config
sources (Consul, ZooKeeper) and ultimately stops the framework from promoting
the custom config sources for the users.
This took my attention as there are clearly 1st and 2nd class config providers.
This RFC proposes a new implementation of the config layer solving the problem 
of config sources inequality and making config layer customizable by flow users.

## Intro

In the simplest form, a config is a simple dictionary. A specific key
corresponds to a value.

```go
config := map[string]string{
	"foo": "bar",
	"baz": "moo",
}
```

It's a flat structure with no nesting, and the data is of a strict `string`
type.

The next level of complexity comes with a concept of composition. In this case a
config structure is no longer flat: it's a tree.

```go
config := map[string]interface{}{
	"foo": map[string]interface{}{
		"bar": 42,
		"baz": "moo",
	},
}
```

The values are now of interface{} type, which provides more flexibility to the
config users. Assume, config has method `Get(key string) interface{}` defined,
which understands key traversal, e.g.:

```go
v := config.Get("foo.bar") // Returns interface{}(42)
```

Now, if `config.Get("foo.bar") == 42` and `config.Get("foo.baz") == "moo"`, what
should be the value for config.Get("foo")? This is the question this RFC
answers: `foo` is a composite value.

```go
v := config.Get("foo") // Returns map[string]interface{}{"bar": 42, "baz": "moo"}
```

This approach is based on a trie implementation. The data structure looks like:

```
     "foo"
    /    \
 "bar"  "baz"
   |      |
  42    "moo"
```

A leaf represents a value.

Now let's assume the config values can come from distinct sources (providers). E.g.:

```go
// provider1
config["foo"] = "bar"

// provider2
config["foo"] = 42
```

Apparently, in this case the last write wins. It does not make sense if all
providers have an equal chance of setting the value. Therefore, I introduced the
concept of parameter weight and made config to resolve the factual value on
flight:

```go
provider1 := Provider{Val: "bar", Weight: 10}
provider2 := Provider{Val: 42, Weight: 20}

config["foo"] = []Provider{provider1, provider2}

v := config.Get("foo") // ???
```

Here comes the trick: when we register a provider, we sort the list based on the
weight, so, it becomes: `config["foo"] = []Provider{provider2, provider1}`

Next, whenever a key lookup happens, providers in the list are being asked for
the value at this moment instead of resolving and memorizing the values upfront.

```go
func (config *config) Get(key string) (interface{}, bool) {
	for _, prov := range config["foo"] {
		if v, ok := prov.Get("foo"); ok {
			return v, true
		}
	}
	return nil, false
}
```

Providers register themselves as "knowing the answer" for every key they are
planning to serve. By the moment of querying, a provider might not have an
answer (yet or already), this is why we jump to the next in the priority list.

This loop is blocking: provider must answer to the lookup or give up, there is
no timeout.

This is only a part of the challenge. The second part comes as the type casting.
Passing maps around is not always an option, especially when it's a matter of
enforcing a specific structure. This approach needs a schema definition in order
to set up the expectations from values stored under specific keys.

I introduced a concept of flexible ad-hoc schema. In short, the idea is to have a schema
defined for every level of the config trie (every leaf and every node). This
approach, applied recursively, builds up composite structures from ground up on
demand.

Assume, there is a schema defined for `"foo.bar" -> Int`. And a corresponding
schema for `"foo.baz" -> String`.
And assume `"foo" -> struct{Bar: Int, Baz: String}`

The data structure won't be built magically: our program doesn't know to build
it up. Thereefore, I introduced a concept of a *config mapper*: a component that
knows how to build a structure from the corresponding attributes. Simply
speaking, a mapper is a function, that given a hashmap `map[string]interface{}{"bar": 42, "baz": "moo"}`,
will return `struct{Bar: 42, Baz: "moo"}` for key `"foo"` lookup. Deeper key
lookups are still valid: `config.Get("foo.bar") -> 42`.

Now let's introduce the concept of schema in this context. A schema is a trie
structure defining a mapper for every level of the trie. The format of the data
is expected to be known in advance.

Effectively, the config structure turns into something like:

```
[root]
  └-["foo" Mapper{ Schema: struct{Bar: Int, Baz: String} }]
      └-["bar" Mapper{ Schema: Int }, Providers: [provider2{Val: 42}, provider1{Val: 0}]]
      └-["baz" Mapper{ Schema: String }, Providers: [provider3{Val: "moo"}]]
```

A "foo" key lookup will look like:

1. Decompose the key into fragments, i.e.: `["foo"]`.
2. For every fragment of the key:
3. Check if foo-node has providers. If yes, goto 5.
4. For every child of foo-node execute 3 recursively.
5. In a provider list loop: check if a provider has a value, break if it does.
6. If the value exists, lookup for the corresponding schema mapper, apply if present.
7. Return the result.

Using this algorithm allows one to build more complex structures from smaller
ones and lets it stay provider agnostic.

Providers might provide values in different formats, say, a yaml provider
provides `system.maxprocs` as an Int, whereas a corresponding environment
variable `SYSTEM_MAXPROCS` will be resolved into a String. For the purpose of
type casting, there is Converter interface. It's a family of convertor units
that know how to convert values of specific types to something else. For
example: converter might know how to convert \*Int to Int, or String to Int.
Converters are similar to Mappers, but I decided to keep them apart for the sake
of emphasizing the idea: a Converter either converts a value or declares it
unknown letting some other converter do the job. A Mapper triggers an error if
conversion fails. Converters were created composable: a set of best-effort units
might be composed in a chain (connected with diferent strategy: say: at least one
of the Converters should be able  to convert the input value, or all of them, or
the last wins). A chain is already a mapper. This gives an idea about the
hierarchy: Converters provide their best effort and only know how to convert
primitives. Mappers encorporate some complex composition logic and use
promitives for trivial conversions.
