package cache

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

const autoReportDuration = time.Minute

type Container interface {
	Query(sha512Hex string) []byte

	Upsert(sha512Hex string, data []byte)
	Remove(sha512Hex string)
	Invalidate()

	Purge()
}

type indexItem struct {
	sha512Hex string
	data      []byte

	expiresAt time.Time
	sortIndex int
}

type indexItemList []*indexItem

func (p indexItemList) Len() int           { return len(p) }
func (p indexItemList) Less(i, j int) bool { return p[i].expiresAt.Before(p[j].expiresAt) }
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
	container.autoReport()

	return container
}

func (c *container) start() {
	// Purge Timer
	go func() {
		for {
			time.Sleep(c.lifetime)

			c.logger.Info("Purging Cache...")
			c.Purge()
			c.logger.Info("Cache Purging is completed")
		}
	}()
}

func (c *container) autoReport() {
	limit := c.limit / (1024 * 1024)

	usageBackup := uint64(0)
	freeBackup := uint64(0)

	go func() {
		for {
			usage := c.usage / (1024 * 1024)
			free := c.limit - c.usage
			free /= 1024 * 1024

			if usageBackup != usage || freeBackup != free {
				c.logger.Info(fmt.Sprintf("Data Node Memory: %dM Used, %dM Free, %dM Total", usage, free, limit))

				usageBackup = usage
				freeBackup = free
			}

			time.Sleep(autoReportDuration)
		}
	}()
}

func (c *container) Query(sha512Hex string) []byte {
	if c.limit == 0 {
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	index, has := c.index[sha512Hex]
	if !has {
		return nil
	}

	c.sortedIndex[index.sortIndex] = nil

	index.expiresAt = time.Now().UTC().Add(c.lifetime)
	index.sortIndex = len(c.sortedIndex)

	c.sortedIndex = append(c.sortedIndex, &index)
	c.index[index.sha512Hex] = index

	return index.data
}

func (c *container) Upsert(sha512Hex string, data []byte) {
	if c.limit == 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if currentItem, has := c.index[sha512Hex]; has {
		c.sortedIndex[currentItem.sortIndex] = nil
		c.usage -= uint64(len(currentItem.data))
		delete(c.index, currentItem.sha512Hex)
	}

	dataSize := uint64(len(data))

	if c.limit < c.usage+dataSize {
		// if system caching is its limit, it is better to trim the 1/4 of its usage
		// for system performance and efficiency.
		c.trimUnsafe(c.usage / 4)
	}

	item := indexItem{
		sha512Hex: sha512Hex,
		data:      make([]byte, len(data)),
		expiresAt: time.Now().UTC().Add(c.lifetime),
		sortIndex: len(c.sortedIndex),
	}
	copy(item.data, data)

	c.sortedIndex = append(c.sortedIndex, &item)
	c.usage += dataSize
	c.index[item.sha512Hex] = item
}

func (c *container) Remove(sha512Hex string) {
	if c.limit == 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	currentItem, has := c.index[sha512Hex]
	if !has {
		return
	}

	c.sortedIndex[currentItem.sortIndex] = nil
	c.usage -= uint64(len(currentItem.data))
	delete(c.index, currentItem.sha512Hex)
}

func (c *container) Invalidate() {
	if c.limit == 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.sortedIndex = make(indexItemList, 0)
	c.index = make(map[string]indexItem)
	c.usage = 0
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

		if indexItem.expiresAt.Before(time.Now().UTC()) {
			c.sortedIndex = append(c.sortedIndex[:i], c.sortedIndex[i+1:]...)
			c.usage -= uint64(len(indexItem.data))
			delete(c.index, indexItem.sha512Hex)
			i--

			continue
		}

		indexItem.sortIndex = currentIndex
		c.index[indexItem.sha512Hex] = *indexItem

		currentIndex++
	}
}

func (c *container) trimUnsafe(size uint64) {
	if c.limit == 0 {
		return
	}

	for i, indexItem := range c.sortedIndex {
		if indexItem == nil {
			continue
		}

		if size <= 0 && indexItem.expiresAt.After(time.Now().UTC()) {
			return
		}

		dataSize := uint64(len(indexItem.data))

		c.sortedIndex[i] = nil
		c.usage -= dataSize
		delete(c.index, indexItem.sha512Hex)

		size -= dataSize
	}
}
