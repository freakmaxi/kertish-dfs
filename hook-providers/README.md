# Kertish DFS Hook Providers

Kertish-dfs supports hooks for folder operation notifications. Hooks are working in a plugins'
manner. So, you can create your own hook provider and use it under the head node.

**You are very welcome developing a hook provider under this path and create a PR.**

All the available providers will be listed in this path. You can find the custom 3rdparty and
official hook providers together. The official hook providers are listed below in this document.

### Official Hook Providers
- rabbitmq

## What is hook provider?

Hook provider is a small plugin that is executed by Kertish-dfs when the folder operation has been done.
The provider can be used to notify the external system for the folder operation, or logging purposes. It
is a simple extension to sync operation between systems.

## How to develop a hook provider?

It is very easy to implement a hook provider. A hook provider has one special function to be able to load
by the Kertish-dfs. `Load`.

`Load` function is returning a `hook.Action` interface, which contains all the information related to the
hook provider

#### Simple Hook Provider Structure
```go
package main

import (
	"encoding/json"

	"github.com/freakmaxi/kertish-dfs/basics/hooks"
)

// Load is an exported function and you do not need to make changes other than the
// struct name. This function is used to have the hooks.Action implemented struct
// on the fly. DO NOT PUT ANY LOGIC IN THIS FUNCTION
func Load() hooks.Action {
	return &MyHookProvider{}
}

// MyHookProvider should be an exported struct. It is the place that you can
// keep your setup to use in execution.
type MyHookProvider struct {
	SetupField1 string `json:"setupField1"`
	SetupField2 string `json:"setupField2"`
}

// Provider returns the name of your provider
func (m *MyHookProvider) Provider() string {
	return "my-hook-provider-name" // Not mandatory but better to keep as a word 
}

func (m *MyHookProvider) Version() string {
	return "your-build-number" // You can decide your own versioning format
}

// Sample returns the sample setup of the provider.
func (m *MyHookProvider) Sample() interface{} {
	return &MyHookProvider{
		SetupField1: "amqp://test:test@127.0.0.1:5672/",
		SetupField2: "testQueueName",
	}
}

// New returns an empty provider where the hooks.Action interface is already implemented
// DO NOT PUT ANY LOGIC IN THIS FUNCTION
func (m *MyHookProvider) New() hooks.Action {
	return Load()
}

// Setup accepts hooks.SetupMap which is basically a map[string]interface{}
// Kertish-dfs will create a new instance using `New` function and call
// Setup function with the data coming from the folder hook registration
// So, this function will fill your `MyHookProvider` fields that are possible
// used in the Execution function
func (m *MyHookProvider) Setup(v hooks.SetupMap) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, m)
}

// Execute takes place your plugin handling the operation.
// it is executed only when the hook definition criteria are met and Kertish-dfs
// operation was successful, such as execute on only a successful deletion
// NOW YOU CAN PUT YOUR LOGIC IN HERE
func (m *MyHookProvider) Execute(aI *hooks.ActionInfo) error {
	return nil
}

var _ hooks.Action = &MyHookProvider{}
```

- You can check the [hooks.ActionInfo](https://github.com/freakmaxi/kertish-dfs/blob/master/basics/hooks/action_info.go)
  to see what kind of input you will have as a parameter in Execute function.
  
### What are the important points?

- The execution of the `Execute` function is happening synchronously. Which means
  if the execution of your plugin takes time, this will decrease the performance of
  Kertish-dfs. 
- If your `Execute` function panics, this will cause a crash in Kertish-dfs. The unexpected
  crash in Kertish-dfs, can cause inconsistency in the dfs and may cause losing data.
- The returning error is just logging purposes. If you have an error, and you return it,
  this will be logged but execution will not be retried.
- There is no chronological guarantee of the `Execute` function call.
- You can register exactly the same hook definition to different folders but every dfs operation
  creates a new hook provider instance for the execution. So, these operations will reach your
  different hook provider instances. Each hook provider instance execution will be done once. 
- If you have a long-running process and may consider making it asynchronously using some go routines but
  Kertish-dfs does not give any guarantee to wait the execution complete. So, your hook provider
  should be quick to finish and should not contain that much complex logic.
