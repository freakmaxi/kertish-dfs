package hooks

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type RunOn int

const (
	All     RunOn = 1 // is executed in anyway
	Created RunOn = 2 // Folder/File or SubFolder/SubFile (if recursive) is newly Created
	Updated RunOn = 3 // Folder/File or SubFolder/File is Copied or Moved (Renamed)
	Deleted RunOn = 4 // Folder/File or SubFolder/SubFile (if recursive) is completely Deleted
)

// SetupMap is the simplified type name for underlying map
type SetupMap map[string]interface{}

// Hook struct holds the information of the action for the Folder base on RunOn setup
// RunOn is the type of the action that can be reason for the execution
// Times is the counter for the allowed executions. -1 is no limit. 0 is execution is not allowed anymore
// Recursive checks if the hook responsible for the sub folders
// Action is the action to take
type Hook struct {
	Id        *string    `json:"id"`
	Created   *time.Time `json:"created"`
	RunOn     RunOn      `json:"runOn" bson:"runOn"`
	Recursive bool       `json:"recursive"`
	Provider  string     `json:"provider"`
	Setup     SetupMap   `json:"setup"`

	action Action
}

// Hooks is the definition of the pointer array of Hook struct
type Hooks []*Hook

func (h *Hook) Prepare() {
	createdAt := time.Now().UTC()
	if h.Created != nil {
		createdAt = *h.Created
	}

	idMap := fmt.Sprintf("%d:%t:%s:%s", h.RunOn, h.Recursive, h.Provider, createdAt.Format(time.RFC3339Nano))
	idHash := md5.Sum([]byte(idMap))
	id := hex.EncodeToString(idHash[:])

	h.Id = &id
	h.Created = &createdAt
}

func (h *Hook) Action() (Action, error) {
	if h.action != nil {
		return h.action, nil
	}

	for _, action := range CurrentLoader.List() {
		if strings.Compare(action.Provider(), h.Provider) != 0 {
			continue
		}

		h.action = action.New()
		if err := h.action.Setup(h.Setup); err != nil {
			return nil, err
		}
		return h.action, nil
	}

	return nil, fmt.Errorf("unable to find the provider")
}
