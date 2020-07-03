package cache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestContainer_PurgeV1(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	container := container{
		limit:       1024 * 1024 * 5,
		lifetime:    time.Second * 60,
		mutex:       &sync.Mutex{},
		index:       make(map[string]indexItem),
		sortedIndex: make(indexItemList, 0),
		logger:      logger,
	} // limit 5MB

	for i := 0; i < 5; i++ {
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
	logger, _ := zap.NewDevelopment()

	container := container{
		limit:       1024 * 1024 * 5,
		lifetime:    time.Second * 5,
		mutex:       &sync.Mutex{},
		index:       make(map[string]indexItem),
		sortedIndex: make(indexItemList, 0),
		logger:      logger,
	} // limit 5MB
	container.start()

	for i := 0; i < 5; i++ {
		container.Upsert(fmt.Sprintf("a%d", i+1), []byte{})
		container.sortedIndex[i].expiresAt = time.Now().UTC().Add(time.Second * 6)
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

	time.Sleep(time.Millisecond * 5200)

	for i := 0; i < 4; i++ {
		assert.Equal(t, i, container.sortedIndex[i].sortIndex)
	}
}

// Memory Test
func TestContainer_PurgeV3(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	container := container{
		limit:       1024 * 1024 * 5,
		lifetime:    time.Second * 60,
		mutex:       &sync.Mutex{},
		index:       make(map[string]indexItem),
		sortedIndex: make(indexItemList, 0),
		logger:      logger,
	} // limit 5MB
	container.start()

	mem := &runtime.MemStats{}
	runtime.ReadMemStats(mem)

	allocated := mem.Alloc

	type dI struct {
		name  string
		data  []byte
		index int
	}

	for i := 0; i < 1024; i++ {
		dI := dI{
			name:  fmt.Sprintf("a%d", i+1),
			data:  make([]byte, 1024*i),
			index: i,
		}

		container.Upsert(dI.name, dI.data)
		container.sortedIndex[i].expiresAt = time.Now().UTC().Add(time.Second * 61)
	}
	runtime.GC()

	runtime.ReadMemStats(mem)
	result := mem.Alloc - allocated

	assert.Equal(t, int64(5227520), int64(container.usage))
	assert.Less(t, int64(result), int64(container.limit))
}
