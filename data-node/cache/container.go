package cache

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type Container interface {
	Query(sha512Hex string) []byte

	Upsert(sha512Hex string, data []byte)
	Remove(sha512Hex string)

	Purge()
}

type indexItem struct {
	sha512Hex string
	data      []byte

	date      time.Time
	sortIndex int
}

type indexItemList []*indexItem

func (p indexItemList) Len() int           { return len(p) }
func (p indexItemList) Less(i, j int) bool { return p[i].date.Before(p[j].date) }
func (p indexItemList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type container struct {
	limit    uint64
	lifetime time.Duration
	logger   *zap.Logger
	usage    uint64

	mutex       *sync.Mutex
	index       map[string]indexItem
	sortedIndex indexItemList
}

func NewContainer(limit uint64, lifetime time.Duration, logger *zap.Logger) Container {
	container := &container{
		limit:       limit,
		lifetime:    lifetime,
		logger:      logger,
		mutex:       &sync.Mutex{},
		index:       make(map[string]indexItem),
		sortedIndex: make(indexItemList, 0),
	}

	if limit == 0 {
		return container
	}
	container.start()

	return container
}

func (c *container) start() {
	// Purge Timer
	go func() {
		for {
			select {
			case <-time.After(c.lifetime):
				c.logger.Info("Purging Cache...")
				c.Purge()
				c.logger.Info("Cache Purging is completed!")
			}
		}
	}()
}

func (c *container) Query(sha512Hex string) []byte {
	if c.limit == 0 {
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if index, has := c.index[sha512Hex]; has {
		index.date = time.Now().UTC()
		return index.data
	}

	return nil
}

func (c *container) Upsert(sha512Hex string, data []byte) {
	if c.limit == 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	item := indexItem{
		sha512Hex: sha512Hex,
		data:      make([]byte, len(data)),
		date:      time.Now().UTC(),
		sortIndex: len(c.sortedIndex),
	}
	copy(item.data, data)

	if currentItem, has := c.index[sha512Hex]; has {
		c.sortedIndex[currentItem.sortIndex] = nil
		c.usage -= uint64(len(currentItem.data))
	}

	dataSize := uint64(len(item.data))

	if c.limit < c.usage+dataSize {
		size := int(c.usage + dataSize - c.limit)
		c.trim(size)
	}

	c.index[sha512Hex] = item
	c.sortedIndex = append(c.sortedIndex, &item)
	c.usage += dataSize
}

func (c *container) Remove(sha512Hex string) {
	if c.limit == 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if currentItem, has := c.index[sha512Hex]; has {
		c.sortedIndex[currentItem.sortIndex] = nil
		c.usage -= uint64(len(currentItem.data))
	}

	delete(c.index, sha512Hex)
}

func (c *container) Purge() {
	if c.limit == 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	currentIndex := 0
	for i := 0; i < len(c.sortedIndex); i++ {
		indexItem := c.sortedIndex[i]
		if indexItem == nil {
			c.sortedIndex = append(c.sortedIndex[:i], c.sortedIndex[i+1:]...)
			i--

			continue
		}

		indexAge :=
			indexItem.date.Add(c.lifetime)
		if indexAge.Before(time.Now().UTC()) {
			c.sortedIndex = append(c.sortedIndex[:i], c.sortedIndex[i+1:]...)
			c.usage -= uint64(len(indexItem.data))
			delete(c.index, indexItem.sha512Hex)
			i--

			continue
		}

		indexItem.sortIndex = currentIndex
		currentIndex++
	}
}

func (c *container) trim(size int) {
	if c.limit == 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, indexItem := range c.sortedIndex {
		if indexItem == nil {
			continue
		}

		dataSize := len(indexItem.data)

		c.sortedIndex[i] = nil
		c.usage -= uint64(dataSize)
		delete(c.index, indexItem.sha512Hex)

		size -= dataSize
		if size <= 0 {
			return
		}
	}
}
