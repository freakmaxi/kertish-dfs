package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestContainer_PurgeV1(t *testing.T) {
	container := container{
		limit:       1024 * 1024 * 5,
		lifetime:    time.Second * 60,
		mutex:       &sync.Mutex{},
		index:       make(map[string]indexItem),
		sortedIndex: make(indexItemList, 0),
	} // limit 5MB

	type dI struct {
		name  string
		data  []byte
		index int
	}

	dIs := make([]dI, 0)
	for i := 0; i < 5; i++ {
		dIs = append(dIs, dI{
			name:  fmt.Sprintf("a%d", i+1),
			data:  []byte{},
			index: i,
		})

		container.Upsert(fmt.Sprintf("a%d", i+1), []byte{})
	}

	for i := 0; i < 5; i++ {
		assert.Equal(t, i, container.sortedIndex[i].sortIndex)
	}
	container.Remove("a3")

	for i := 0; i < 5; i++ {
		if i == 2 {
			assert.Nil(t, container.sortedIndex[i])
			continue
		}
		assert.Equal(t, i, container.sortedIndex[i].sortIndex)
	}

	container.Purge()

	for i := 0; i < 4; i++ {
		assert.Equal(t, i, container.sortedIndex[i].sortIndex)
	}
}

func TestContainer_PurgeV2(t *testing.T) {
	container := container{
		limit:       1024 * 1024 * 5,
		lifetime:    time.Second * 5,
		mutex:       &sync.Mutex{},
		index:       make(map[string]indexItem),
		sortedIndex: make(indexItemList, 0),
	} // limit 5MB
	container.start()

	type dI struct {
		name  string
		data  []byte
		index int
	}

	dIs := make([]dI, 0)
	for i := 0; i < 5; i++ {
		dIs = append(dIs, dI{
			name:  fmt.Sprintf("a%d", i+1),
			data:  []byte{},
			index: i,
		})

		container.Upsert(fmt.Sprintf("a%d", i+1), []byte{})
		container.sortedIndex[i].date = time.Now().UTC().Add(time.Second)
	}

	for i := 0; i < 5; i++ {
		assert.Equal(t, i, container.sortedIndex[i].sortIndex)
	}
	container.Remove("a3")

	for i := 0; i < 5; i++ {
		if i == 2 {
			assert.Nil(t, container.sortedIndex[i])
			continue
		}
		assert.Equal(t, i, container.sortedIndex[i].sortIndex)
	}

	<-time.After(time.Second * 5)

	for i := 0; i < 4; i++ {
		assert.Equal(t, i, container.sortedIndex[i].sortIndex)
	}
}
