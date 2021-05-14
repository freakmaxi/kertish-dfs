package hooks

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
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

// Hook struct holds the information of the action for the Folder base on RunOn setup
// RunOn is the type of the action that can be reason for the execution
// Times is the counter for the allowed executions. -1 is no limit. 0 is execution is not allowed anymore
// Recursive checks if the hook responsible for the sub folders
// Action is the action to take
type Hook struct {
	Id        string     `json:"id"`
	CreatedAt *time.Time `json:"createdAt"`

	RunOn     RunOn  `json:"runOn"`
	Recursive bool   `json:"recursive"`
	Action    Action `json:"action"`
}

// Hooks is the definition of the pointer array of Hook struct
type Hooks []*Hook

func (h *Hook) update() {
	if h.CreatedAt == nil {
		createdAt := time.Now().UTC()
		h.CreatedAt = &createdAt
	}

	id := fmt.Sprintf("%d:%t:%s:%s", h.RunOn, h.Recursive, h.Action.Provider(), h.CreatedAt.Format(time.RFC3339Nano))
	idHash := md5.Sum([]byte(id))
	h.Id = hex.EncodeToString(idHash[:])
}

func (h *Hook) MarshalJSON() ([]byte, error) {
	h.update()

	return json.Marshal(&struct {
		Id        string          `json:"id"`
		CreatedAt *time.Time      `json:"createdAt"`
		RunOn     int             `json:"runOn"`
		Recursive bool            `json:"recursive"`
		Provider  string          `json:"provider"`
		Action    json.RawMessage `json:"action"`
	}{
		Id:        h.Id,
		CreatedAt: h.CreatedAt,
		RunOn:     int(h.RunOn),
		Recursive: h.Recursive,
		Provider:  h.Action.Provider(),
		Action:    h.Action.Serialize(),
	})
}

func (h *Hook) UnmarshalJSON(data []byte) error {
	r := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &r); err != nil {
		return err
	}

	if err := json.Unmarshal(r["id"], &h.Id); err != nil {
		return err
	}
	if err := json.Unmarshal(r["createdAt"], &h.CreatedAt); err != nil {
		return err
	}
	if err := json.Unmarshal(r["runOn"], &h.RunOn); err != nil {
		return err
	}
	if err := json.Unmarshal(r["recursive"], &h.Recursive); err != nil {
		return err
	}
	var provider string
	if err := json.Unmarshal(r["provider"], &provider); err != nil {
		return err
	}

	for _, action := range CurrentLoader.List() {
		if strings.Compare(action.Provider(), provider) != 0 {
			continue
		}

		h.Action = action.New()
		return h.Action.Create(r["action"])
	}

	return fmt.Errorf("unable to find the provider")
}
