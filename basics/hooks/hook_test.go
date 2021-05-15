package hooks

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testAction struct {
	ConnectionUrl   string `json:"connectionUrl"`
	TargetQueueName string `json:"targetQueueName"`
}

func (r *testAction) Provider() string {
	return "testaction"
}

func (r *testAction) Version() string {
	return "XX.X.XXXX"
}

func (r *testAction) Sample() interface{} {
	return &testAction{
		ConnectionUrl:   "amqp://test:test@127.0.0.1:5672/",
		TargetQueueName: "testQueueName",
	}
}

func (r *testAction) New() Action {
	return &testAction{}
}

func (r *testAction) Setup(v SetupMap) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, r)
}

func (r *testAction) Execute(ai *ActionInfo) error {
	fmt.Println(ai)
	return nil
}

var _ Action = &testAction{}

var testHook = &Hook{
	RunOn:     All,
	Recursive: true,
	Provider:  "testaction",
	Setup: map[string]interface{}{
		"connectionUrl":   "amqp://test:test@127.0.0.1:5672/",
		"targetQueueName": "testQueueName",
	},
}
var hookJsonString string

type testLoader struct {
}

func (t testLoader) HooksPath() string {
	return "./hooks"
}

func (t testLoader) List() []Action {
	return []Action{&testAction{}}
}

func (t testLoader) Get(name string) Action {
	return &testAction{}
}

var _ Loader = &testLoader{}

func loadPlugin() {
	CurrentLoader = &testLoader{}
}

func prepare() {
	testHook.Prepare()

	b, _ := json.Marshal(testHook)
	hookJsonString = string(b)
}

func init() {
	loadPlugin()
	prepare()
}

func TestHook_UnmarshalJSON(t *testing.T) {
	b := []byte(hookJsonString)

	h := &Hook{}
	err := json.Unmarshal(b, h)

	assert.Nil(t, err)
	assert.Equal(t, testHook, h)
}
