package data

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Operation interface {
	RepairDetail() (RepairDetail, error)
	SetRepairing(repairing bool, completed bool) error
}

type operation struct {
	mutex *sync.Mutex

	client    CacheClient
	keyPrefix string
}

type RepairDetail struct {
	Processing bool
	Timestamp  *time.Time
}

func NewOperation(client CacheClient, keyPrefix string) Operation {
	return &operation{
		client:    client,
		keyPrefix: keyPrefix,
		mutex:     &sync.Mutex{},
	}
}

func (o *operation) key(name string) string {
	return fmt.Sprintf("%s_operation_%s", o.keyPrefix, name)
}

func (o *operation) RepairDetail() (RepairDetail, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	v, err := o.client.HGetAll(o.key("repairing"))
	if v == nil || err != nil {
		return RepairDetail{Processing: false}, err
	}

	processing, has := v["processing"]
	if !has {
		processing = "false"
	}

	var timestamp *time.Time
	if t, has := v["timestamp"]; has {
		_t, err := time.Parse(time.RFC3339, t)
		if err == nil {
			timestamp = &_t
		}
	}

	return RepairDetail{
		Processing: strings.Compare(processing, "true") == 0,
		Timestamp:  timestamp,
	}, nil
}

func (o *operation) SetRepairing(repairing bool, completed bool) error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	content := make(map[string]string)
	content["processing"] = strconv.FormatBool(repairing)
	content["timestamp"] = ""

	if completed {
		content["timestamp"] = time.Now().UTC().Format(time.RFC3339)
	}

	return o.client.HMSet(o.key("repairing"), content)
}

var _ Operation = &operation{}
