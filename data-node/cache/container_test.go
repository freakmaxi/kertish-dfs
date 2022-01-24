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
		container.Upsert(fmt.Sprintf("a%d", i+1), 0, 0, []byte{})
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
		container.Upsert(fmt.Sprintf("a%d", i+1), 0, 0, []byte{})
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

		container.Upsert(dI.name, 0, 0, dI.data)
		container.sortedIndex[i].expiresAt = time.Now().UTC().Add(time.Second * 61)
	}
	runtime.GC()

	runtime.ReadMemStats(mem)
	result := mem.Alloc - allocated

	assert.Equal(t, int64(4184064), container.usage)
	assert.Less(t, int64(result), int64(container.limit))
}

func TestIndexItem_MatchRangeV1(t *testing.T) {
	item := indexItem{
		sha512Hex: "test",
		expiresAt: time.Now(),
		sortIndex: 0,

		dataItems: []dataContainer{
			{
				begins: 21,
				ends:   29,
				data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
			},
			{
				begins: 30,
				ends:   35,
				data:   []byte{9, 10, 11, 12, 13, 14},
			},
			{
				begins: 37,
				ends:   40,
				data:   []byte{16, 17, 18, 19},
			},
			{
				begins: 41,
				ends:   44,
				data:   []byte{20, 21, 23, 24},
			},
			{
				begins: 45,
				ends:   50,
				data:   []byte{25, 26, 27, 28, 29, 30},
			},
		},
	}

	result1 := item.MatchRange(24, 28)
	assert.Equal(t, []byte{3, 4, 5, 6, 7}, result1)

	result2 := item.MatchRange(21, 29)
	assert.Equal(t, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8}, result2)

	result3 := item.MatchRange(28, 33)
	assert.Equal(t, []byte{7, 8, 9, 10, 11, 12}, result3)

	result4 := item.MatchRange(28, 38)
	assert.Nil(t, result4)

	result5 := item.MatchRange(37, 50)
	assert.Equal(t, []byte{16, 17, 18, 19, 20, 21, 23, 24, 25, 26, 27, 28, 29, 30}, result5)

	result6 := item.MatchRange(19, 28)
	assert.Nil(t, result6)

	result7 := item.MatchRange(37, 51)
	assert.Nil(t, result7)

	result8 := item.MatchRange(0, 0)
	assert.Nil(t, result8)
}

func TestIndexItem_MatchRangeV2(t *testing.T) {
	item := indexItem{
		sha512Hex: "test",
		expiresAt: time.Now(),
		sortIndex: 0,

		dataItems: []dataContainer{
			{
				begins: 0,
				ends:   0,
				data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
			},
		},
	}

	result1 := item.MatchRange(0, 0)
	assert.Equal(t, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8}, result1)

	result2 := item.MatchRange(0, 9)
	assert.Equal(t, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8}, result2)

	result3 := item.MatchRange(0, 10)
	assert.Nil(t, result3)
}

func TestIndexItem_MatchRangeV3(t *testing.T) {
	item := indexItem{
		sha512Hex: "test",
		expiresAt: time.Now(),
		sortIndex: 0,

		dataItems: []dataContainer{
			{
				begins: 10,
				ends:   19,
				data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
			},
		},
	}

	result1 := item.MatchRange(8, 21)
	assert.Nil(t, result1)

	result2 := item.MatchRange(8, 3)
	assert.Nil(t, result2)
}

func TestIndexItem_MergeV1(t *testing.T) {
	item := indexItem{
		sha512Hex: "test",
		expiresAt: time.Now(),
		sortIndex: 0,

		dataItems: []dataContainer{
			{
				begins: 21,
				ends:   29,
				data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
			},
			{
				begins: 24,
				ends:   28,
				data:   []byte{3, 4, 5, 6, 7},
			},
			{
				begins: 27,
				ends:   32,
				data:   []byte{6, 7, 8, 9, 10, 11},
			},
			{
				begins: 41,
				ends:   44,
				data:   []byte{20, 21, 23, 24},
			},
			{
				begins: 45,
				ends:   50,
				data:   []byte{25, 26, 27, 28, 29, 30},
			},
		},
	}

	item.Merge(33, 39, []byte{12, 13, 14, 15, 16, 17, 18})

	assert.Equal(t,
		dataContainer{
			data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			begins: 21,
			ends:   32,
		},
		item.dataItems[0],
	)

	assert.Equal(t,
		dataContainer{
			data:   []byte{12, 13, 14, 15, 16, 17, 18},
			begins: 33,
			ends:   39,
		},
		item.dataItems[1],
	)

	assert.Equal(t,
		dataContainer{
			begins: 41,
			ends:   44,
			data:   []byte{20, 21, 23, 24},
		},
		item.dataItems[2],
	)

	assert.Equal(t,
		dataContainer{
			begins: 45,
			ends:   50,
			data:   []byte{25, 26, 27, 28, 29, 30},
		},
		item.dataItems[3],
	)
}

func TestIndexItem_MergeV2(t *testing.T) {
	item := indexItem{
		sha512Hex: "test",
		expiresAt: time.Now(),
		sortIndex: 0,

		dataItems: []dataContainer{
			{
				begins: 21,
				ends:   29,
				data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
			},
			{
				begins: 24,
				ends:   28,
				data:   []byte{3, 4, 5, 6, 7},
			},
			{
				begins: 27,
				ends:   32,
				data:   []byte{6, 7, 8, 9, 10, 11},
			},
			{
				begins: 41,
				ends:   44,
				data:   []byte{20, 21, 23, 24},
			},
			{
				begins: 45,
				ends:   50,
				data:   []byte{25, 26, 27, 28, 29, 30},
			},
		},
	}

	item.Merge(29, 35, []byte{8, 9, 10, 11, 12, 13, 14})

	assert.Equal(t,
		dataContainer{
			data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
			begins: 21,
			ends:   35,
		},
		item.dataItems[0],
	)

	assert.Equal(t,
		dataContainer{
			begins: 41,
			ends:   44,
			data:   []byte{20, 21, 23, 24},
		},
		item.dataItems[1],
	)

	assert.Equal(t,
		dataContainer{
			begins: 45,
			ends:   50,
			data:   []byte{25, 26, 27, 28, 29, 30},
		},
		item.dataItems[2],
	)
}

func TestIndexItem_MergeV3(t *testing.T) {
	item := indexItem{
		sha512Hex: "test",
		expiresAt: time.Now(),
		sortIndex: 0,

		dataItems: []dataContainer{
			{
				begins: 21,
				ends:   29,
				data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
			},
			{
				begins: 24,
				ends:   28,
				data:   []byte{3, 4, 5, 6, 7},
			},
			{
				begins: 27,
				ends:   32,
				data:   []byte{6, 7, 8, 9, 10, 11},
			},
			{
				begins: 41,
				ends:   44,
				data:   []byte{20, 21, 23, 24},
			},
			{
				begins: 45,
				ends:   50,
				data:   []byte{25, 26, 27, 28, 29, 30},
			},
		},
	}

	item.Merge(21, 50, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23, 24, 25, 26, 27, 28, 29, 30})

	assert.Equal(t,
		dataContainer{
			data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23, 24, 25, 26, 27, 28, 29, 30},
			begins: 21,
			ends:   50,
		},
		item.dataItems[0],
	)
}

func TestIndexItem_MergeV4(t *testing.T) {
	item := indexItem{
		sha512Hex: "test",
		expiresAt: time.Now(),
		sortIndex: 0,

		dataItems: []dataContainer{
			{
				begins: 21,
				ends:   29,
				data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
			},
			{
				begins: 24,
				ends:   28,
				data:   []byte{3, 4, 5, 6, 7},
			},
			{
				begins: 27,
				ends:   32,
				data:   []byte{6, 7, 8, 9, 10, 11},
			},
			{
				begins: 41,
				ends:   44,
				data:   []byte{20, 21, 23, 24},
			},
			{
				begins: 45,
				ends:   50,
				data:   []byte{25, 26, 27, 28, 29, 30},
			},
		},
	}

	item.Merge(0, 0, []byte{0, 1, 2, 3, 4, 5, 6})

	assert.Equal(t,
		dataContainer{
			data:   []byte{0, 1, 2, 3, 4, 5, 6},
			begins: 0,
			ends:   0,
		},
		item.dataItems[0],
	)
}

func TestIndexItem_MergeV5(t *testing.T) {
	item := indexItem{
		sha512Hex: "test",
		expiresAt: time.Now(),
		sortIndex: 0,

		dataItems: []dataContainer{
			{
				begins: 21,
				ends:   29,
				data:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
			},
			{
				begins: 24,
				ends:   28,
				data:   []byte{3, 4, 5, 6, 7},
			},
			{
				begins: 27,
				ends:   32,
				data:   []byte{6, 7, 8, 9, 10, 11},
			},
			{
				begins: 41,
				ends:   44,
				data:   []byte{20, 21, 23, 24},
			},
			{
				begins: 45,
				ends:   50,
				data:   []byte{25, 26, 27, 28, 29, 30},
			},
		},
	}

	oldSize, newSize := item.Merge(0, 0, []byte{0, 1, 2, 3, 4, 5, 6})

	assert.Equal(t, uint64(30), oldSize)
	assert.Equal(t, uint64(7), newSize)
}
