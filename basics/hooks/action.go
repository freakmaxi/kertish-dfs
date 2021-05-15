package hooks

// Action is the interface for the hook operation execution
type Action interface {
	Provider() string
	Version() string
	Sample() interface{}

	New() Action
	Setup(v SetupMap) error

	Execute(aI *ActionInfo) error
}
