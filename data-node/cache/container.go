package cache

import (
	"sort"
	"sync"
	"time"
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
}

type indexItemList []indexItem

func (p indexItemList) Len() int           { return len(p) }
func (p indexItemList) Less(i, j int) bool { return p[i].date.Before(p[j].date) }
func (p indexItemList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type container struct {
	limit    uint64
	usage    uint64
	lifetime time.Duration

	mutex *sync.Mutex
	index map[string]indexItem
}

func NewContainer(limit uint64, lifetime time.Duration) Container {
	container := &container{
		limit:    limit,
		lifetime: lifetime,
		mutex:    &sync.Mutex{},
		index:    make(map[string]indexItem),
	}

	if limit == 0 {
		return container
	}

	// Purge Timer
	go func() {
		for {
			select {
			case <-time.After(lifetime):
				container.Purge()
			}
		}
	}()

	return container
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
	}
	copy(item.data, data)

	if currentItem, has := c.index[sha512Hex]; has {
		c.usage -= uint64(len(currentItem.data))
	}

	dataSize := uint64(len(item.data))

	if c.limit < c.usage+dataSize {
		size := int(c.usage + dataSize - c.limit)
		c.trim(size)
	}

	c.index[sha512Hex] = item
	c.usage += dataSize
}

func (c *container) Remove(sha512Hex string) {
	if c.limit == 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if currentItem, has := c.index[sha512Hex]; has {
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

	indexItemList := make(indexItemList, len(c.index))
	for _, indexItem := range c.index {
		indexItemList = append(indexItemList, indexItem)
	}
	sort.Sort(indexItemList)

	for _, indexItem := range indexItemList {
		indexAge :=
			indexItem.date.Add(c.lifetime)
		if time.Now().UTC().Before(indexAge) {
			return
		}

		c.usage -= uint64(len(indexItem.data))
		delete(c.index, indexItem.sha512Hex)
	}
}

func (c *container) trim(size int) {
	if c.limit == 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	indexItemList := make(indexItemList, len(c.index))
	for _, indexItem := range c.index {
		indexItemList = append(indexItemList, indexItem)
	}
	sort.Sort(indexItemList)

	for _, indexItem := range indexItemList {
		dataSize := len(indexItem.data)

		c.usage -= uint64(dataSize)
		delete(c.index, indexItem.sha512Hex)

		size -= dataSize
		if size <= 0 {
			return
		}
	}
}
