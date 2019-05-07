# RFC#1 Flow Configuration Framework

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

In the simplest form, a config is a simple dictionary, where a specific key corresponds to a value:

```go
config := map[string]string{
	"foo": "bar",
	"baz": "moo",
}
```

It's a flat structure with no nesting, and stored values are of a specific type (`string` in this case).

The next level of complexity comes with a concept of composition. Say, we have a config structure that is no longer flat:

```go
config := map[string]interface{}{
	"foo": map[string]interface{}{
		"bar": 42,
		"baz": "moo",
	},
}
```

This structure relaxes requirements to the stored data structures by specifying it's type as `interface{}`. Let's assume the config has a method `Get(key string) interface{}` defined:

```go
v := config.Get("foo.bar") // Returns interface{}(42)
```

Now, if `config.Get("foo.bar") == 42` and `config.Get("foo.baz") == "moo"`, what
should be the value for `config.Get("foo")`?

It's an open question, and we solve it by introducing a concept of nesting composite data structures. It means that `foo` value effectively encorporates `foo.bar` and `foo.baz` values.

```go
v := config.Get("foo") // Returns map[string]interface{}{"bar": 42, "baz": "moo"}
```

This can be represented as a trie data structure, where leafs represent primitive values and a higher-level nodes lookup returns a composite structure:

```
     "foo"
    /    \
 "bar"  "baz"
   |      |
  42    "moo"
```

So far, there was only 1 source of truth for the stored data. `foo.bar` corresponds to 42, and `foo.baz` corresponds to "moo". Imagine, we have multiple sources of truth, i.e. there are 2 data providers that know the answer to the question: "what's the value for `foo.bar`?". Provider1 says it's 42, provider2 prompts 7. Which one is correct?

```go
// provider1
provider1.config["foo.bar"] = 42

// provider2
provider2.config["foo.bar"] = 7
```

This situation might be resolved in multiple ways. Say, the easiest one is: last answer wins. In this case the value depends on what provider answers the question the last. Or, the other way around: first answer wins.

We decided to take neither of these approaches. We decided to introduce providers weight, which makes the process of the value resolution to be deterministic. If `foo.bar` is provided by both providers, we prefer the one that comes from a provider with a higher weight.

We also added a bit of functional programming and made the value resolution to be lazy: instead of storing actual values, we store references to providers, which resolve the answer on demand.

```go
provider1 := Provider{Val: 42, Weight: 10}
provider2 := Provider{Val: 7, Weight: 20}

config["foo.bar"] = []Provider{provider1, provider2}

v := config.Get("foo.bar") // ???
```

When a provider is being registered, we sort the list based on weights, so it becomes: `config["foo.bar"] = []Provider{provider2, provider1}`. In this case, a value resolution becomes qute straightforward:

```go
func (config *config) Get(key string) (interface{}, bool) {
	for _, prov := range config[key] {
		if v, ok := prov.Get(key); ok {
			return v, true
		}
	}
	return nil, false
}
```

We decided to make the config resolution flexible. As it's been mentioned, the value lookup process is lazy. We tolerate that a provider, being registered under a specific key, might have no value for it by the moment it's asked for it. this is why this loop in the snippet above is there: we return the first answer, ranking them by weights.

A provider is expected to perform a lookup instantly; long-taking queries with no pre-caching are discouraged.

This is only a part of the challenge. The second part comes as the type casting.
Passing maps around is not always an option, especially when it's a matter of enforcing a contract between producers and clients. This approach needs a schema definition in order
to set up the expectations from values stored under specific keys.

We introduced a concept of flexible ad-hoc schema. In short, the idea is to have a schema
defined for every level of the config trie (every leaf and every node). This
approach, applied recursively, builds up composite structures from ground up on
demand.

Say, there is a schema defined for `"foo.bar" -> Int`. And a corresponding
schema for `"foo.baz" -> String`.
And assume `"foo" -> struct{Bar: Int, Baz: String}`

The data structure won't be built magically: our program doesn't know to convert primitives into a struct. Thereefore, we introduced a concept of a *config mapper*: a component that
knows how to build a structure from corresponding primitives. Simply
put, a mapper is a function, that takes a hashmap `map[string]interface{}{"bar": 42, "baz": "moo"}` and returns `struct{Bar: 42, Baz: "moo"}` for key `"foo"` lookup. Deeper key
lookups are still valid: `config.Get("foo.bar") -> 42`.

Now let's introduce the concept of schema in this context. A schema is a structure defining a mapper for every level of the trie. The format of the data
is expected to be known in advance.

Effectively, the config structure turns into something like:

```
[root]
  └-["foo" Mapper{ Schema: struct{Bar: Int, Baz: String} }]
      └-["bar" Mapper{ Schema: Int }, Providers: [provider2{Val: 7}, provider1{Val: 42}]]
      └-["baz" Mapper{ Schema: String }, Providers: [provider3{Val: "moo"}]]
```

`foo` key lookup will look like:

1. Decompose the key into fragments, i.e.: `["foo"]`.
2. For every fragment of the key:
3. Check if foo-node has providers. If yes, goto 5.
4. For every child of foo-node execute 3 recursively.
5. In a provider list loop: check if a provider has a value, break if it does.
6. If the value exists, lookup for the corresponding schema mapper, apply if present.
7. Return the result.

Using this algorithm allows us to build more complex structures from smaller ones and stay provider agnostic.

Providers might provide original values in different types, say, a yaml provider
serves `system.maxprocs` as an Int, whereas a corresponding environment
variable `SYSTEM_MAXPROCS` will be resolved into a String. 

For the purpose of
type casting, there is Converter interface. It's a family of convertor units
that hold the knowledge how to convert values of specific types to something else. For
example: converter might know how to convert `*Int` to `Int`, or `String` to `Int`.


Converters are similar to Mappers, but we decided to keep them in a separate class for the sake
of emphasizing the idea: a Converter either converts a value or declares it
unknown, letting some other converter do the job. A Mapper triggers an error if
conversion fails.

Converters were created composable: a set of best-effort units
might be composed in a chain (connected with diferent strategy: say: at least one
of the Converters should be able to convert the input value, or all of them, or
the last wins). If a chain of converters fails to convert a value, it should fail: there is no last resort plan and we clearly got an unknown value. It makes sense to wrap a chain in  mapper.

This gives an idea about the
hierarchy: Converters provide their best effort and only know how to convert
primitives. Mappers encorporate some complex composition logic and use
primitives for conversions.

Let's look at a chain that performs Int casting. Say, providers can deliver an `Int` value as either: `Int`, `*Int` or `String`. The end goal is to make sure that clients can safely cast the returned value to `Int`.

```go

var baz *int;

config := map[string]interface{}{
	"foo": map[string]interface{
		"bar": []Provider{
			Provider{Val: 42, Weight: 10}, // Plain int
		},
		"baz": []Provider{
			Provider{Val: baz, Weight: 20}, // Pointer to int
			Provider{Val: 0xABADBABE, Weight: 12}, // Also a plain int, for the same key
		},
	},
	"moo": []Provider{
		Provider{Val: "7", Weight: 15}, // Stringified int
	},
}

*baz = 123

IntOrIntPtr := NewCompositeConverter(CompOr, IfInt, IntPtrToInt) // copied from pkg/cast/converter.go; IfInt simply checks if conversion is even needed; IntPtrToInt does an actual conversion from *Int to Int
ToInt := NewCompositeConverter(CompOr, IntOrIntPtr, StrToInt) // copied from pkg/cast/converter.go; CompOr indicates the chain must be using Or logic: first answer wins; note IntOrIntPtr is a composite converter too

schema := map[string]interface{}{
	"foo": {
		"bar": ToInt,
		"baz": ToInt,
	},
	"moo": ToInt,
}

repository.SetData(config) // a component encorporating data lookup logic and schema-based casting
repository.SetSchema(schema)

fooBar, ok := repository.Get("foo.bar") // returns: int(42), true
fooBaz, ok := repository.Get("foo.baz") // returns: int(123), true; the value comes from the 1st provider in the list, serving `baz` variable and resolving it's value on the flight
moo, ok := repository.Get("moo") // returns: int(7), true; StrToInt converter picked it up and converter the value
```

## Conclusion

The presented framework allows flow to serve config data from multiple sources and stay config provider-agnostic. This opens up a lot of space for the new config storage sources, including: ZooKeeper, Consul, JSON API and others.

The schema conversion approach eliminates the necessity for config readers to keep the conversion logic on their side and therefore abstracts clients from the internal specifics of config providers.

Schema conversion removes 1st and 2nd class providers, that used to serve some blocks of config exclusively (e.g.: yaml provider was the only source for pipeline components definition).

This framework should be useful for use in client plugins. There might be as many repositories as needed, each can has it's own schema definition and conversion logic.

Converters and Mappers are stored as simple values and might be passed around and reused easily.

In the upcoming versions of flow we are planning to promote custom config early resolution, which means a user-defined config provider would be able to serve flow bootstrap-stage configs.