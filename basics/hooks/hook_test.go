package hooks

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

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

func (r *testAction) Create(v json.RawMessage) error {
	return json.Unmarshal(v, r)
}

func (r *testAction) Serialize() json.RawMessage {
	b, err := json.Marshal(r)
	if err != nil {
		return nil
	}
	return b
}

func (r *testAction) Execute(ai *ActionInfo) error {
	fmt.Println(ai)
	return nil
}

var _ Action = &testAction{}

var testHook = &Hook{
	RunOn:     All,
	Recursive: true,
	Action: &testAction{
		ConnectionUrl:   "amqp://test:test@127.0.0.1:5672/",
		TargetQueueName: "testQueueName",
	},
}
var hookJsonString string

type testLoader struct {
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
	createdAt := time.Now().UTC()
	testHook.CreatedAt = &createdAt

	id := fmt.Sprintf("%d:%t:%s:%s", testHook.RunOn, testHook.Recursive, testHook.Action.Provider(), testHook.CreatedAt.Format(time.RFC3339Nano))
	idHash := md5.Sum([]byte(id))
	testHook.Id = hex.EncodeToString(idHash[:])

	b, _ := json.Marshal(testHook)
	hookJsonString = string(b)
}

func init() {
	loadPlugin()
	prepare()
}

func TestHook_MarshalJSON(t *testing.T) {
	b, err := json.Marshal(testHook)

	assert.Nil(t, err)
	assert.Equal(t, hookJsonString, string(b))
}

func TestHook_UnmarshalJSON(t *testing.T) {
	b := []byte(hookJsonString)

	h := &Hook{}
	err := json.Unmarshal(b, h)

	assert.Nil(t, err)
	assert.Equal(t, testHook, h)
}
