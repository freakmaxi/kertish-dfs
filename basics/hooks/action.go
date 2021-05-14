package hooks

import "encoding/json"

// Action is the interface for the hook operation execution
type Action interface {
	Provider() string
	Version() string
	Sample() interface{}

	New() Action
	Create(v json.RawMessage) error
	Serialize() json.RawMessage

	Execute(aI *ActionInfo) error
}
