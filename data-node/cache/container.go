package cache

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

const autoReportDuration = time.Minute

type Container interface {
	Query(sha512Hex string, begins uint32, ends uint32) []byte

	Upsert(sha512Hex string, begins uint32, ends uint32, data []byte)
	Remove(sha512Hex string)
	Invalidate()

	Purge()
}

type dataContainer struct {
	begins uint32
	ends   uint32
	data   []byte
}

func (d *dataContainer) Size() uint32 {
	return uint32(len(d.data))
}

type dataContainerList []dataContainer

func (p dataContainerList) Len() int           { return len(p) }
func (p dataContainerList) Less(i, j int) bool { return p[i].begins < p[j].begins }
func (p dataContainerList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type indexItem struct {
	sha512Hex string
	dataItems dataContainerList

	expiresAt time.Time
	sortIndex int
}

type indexItemList []*indexItem

func (p indexItemList) Len() int           { return len(p) }
func (p indexItemList) Less(i, j int) bool { return p[i].expiresAt.Before(p[j].expiresAt) }
func (p indexItemList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (i *indexItem) MatchRange(begins uint32, ends uint32) []byte {
	compiledData := make([]byte, 0)

	for _, dC := range i.dataItems {
		size := uint32(len(dC.data))

		if dC.begins == 0 && dC.ends == 0 {
			if begins == 0 && (ends == 0 || size-ends == 0) {
				return dC.data
			}
			if int(size)-int(ends) < 0 {
				return nil
			}
			return dC.data[begins:ends]
		}

		if begins >= dC.begins && begins < dC.ends {
			startIndex := begins - dC.begins
			remainSize := dC.Size() - startIndex
			if ends < dC.ends {
				remainSize = ends - begins
			}

			compiledData = append(compiledData, dC.data[startIndex:startIndex+remainSize]...)
			begins += remainSize

			if begins == ends {
				return compiledData
			}
		}
	}

	return nil
}

// Merge returns current size and new size after merge
// ends is the ending index and not included
// begins = 6, ends: 10 length should be 4 indices are 6 7 8 9
func (i *indexItem) Merge(begins uint32, ends uint32, data []byte) (uint64, uint64) {
	currentSize := i.Size()

	// if merge request is for whole file, just drop all data pieces and set the whole data as cache
	if begins == 0 && ends == 0 {
		i.dataItems = []dataContainer{
			{
				begins: begins,
				ends:   ends,
				data:   data,
			},
		}
		return currentSize, i.Size()
	}

	// Insert data container blindly to the index
	newDC := dataContainer{
		data:   make([]byte, len(data)),
		begins: begins,
		ends:   ends,
	}
	copy(newDC.data, data)

	i.dataItems = append(i.dataItems, newDC)

	// sort data to process efficiently
	sort.Sort(i.dataItems)

	var mergingContainer *dataContainer

	for idx := 0; idx < len(i.dataItems); idx++ {
		if mergingContainer == nil {
			mergingContainer = &i.dataItems[idx]
			continue
		}

		dC := i.dataItems[idx]

		if dC.begins == 0 && dC.ends == 0 {
			break
		}

		/*
		* Covers these two conditions
		* Condition One
		* ------
		*   ---
		* Condition Two
		* ------
		*    -----
		 */
		if dC.begins <= mergingContainer.ends {
			remainsBegins := dC.begins + (mergingContainer.ends - dC.begins)
			if remainsBegins >= dC.ends {
				i.dataItems = append(i.dataItems[0:idx], i.dataItems[idx+1:]...)
				idx--

				continue
			}

			mergingContainer.data = append(mergingContainer.data, dC.data[remainsBegins-dC.begins:]...)
			mergingContainer.ends += dC.ends - remainsBegins

			i.dataItems = append(i.dataItems[0:idx], i.dataItems[idx+1:]...)
			idx--

			continue
		}

		mergingContainer = nil
	}

	return currentSize, i.Size()
}

func (i *indexItem) Size() uint64 {
	size := uint64(0)
	for _, dC := range i.dataItems {
		size += uint64(dC.Size())
	}
	return size
}

type container struct {
	limit    uint64
	lifetime time.Duration
	logger   *zap.Logger
	usage    int64 // Because of some calculations in place, usage should accept negative numbers

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

	usageBackup := int64(0)
	freeBackup := int64(0)

	go func() {
		for {
			usage := c.usage / (1024 * 1024)
			free := int64(c.limit) - c.usage
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

func (c *container) Query(sha512Hex string, begins uint32, ends uint32) []byte {
	if c.limit == 0 {
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	index, has := c.index[sha512Hex]
	if !has {
		return nil
	}

	data := index.MatchRange(begins, ends)
	if data == nil {
		return nil
	}

	c.sortedIndex[index.sortIndex] = nil

	index.sortIndex = len(c.sortedIndex)
	index.expiresAt = time.Now().UTC().Add(c.lifetime)

	c.sortedIndex = append(c.sortedIndex, &index)
	c.index[index.sha512Hex] = index

	return data
}

func (c *container) Upsert(sha512Hex string, begins uint32, ends uint32, data []byte) {
	if c.limit == 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	dataSize := int64(len(data))

	if c.limit < uint64(c.usage+dataSize) {
		// if system caching is its limit, it is better to trim the 1/4 of its usage
		// for system performance and efficiency.
		c.trimUnsafe(c.usage / 4)
	}

	currentItem, has := c.index[sha512Hex]
	if !has {
		currentItem = indexItem{
			sha512Hex: sha512Hex,
			dataItems: make(dataContainerList, 0),
		}
	} else {
		c.sortedIndex[currentItem.sortIndex] = nil
	}
	currentItem.sortIndex = len(c.sortedIndex)
	currentItem.expiresAt = time.Now().UTC().Add(c.lifetime)

	prevSize, newSize := currentItem.Merge(begins, ends, data)

	c.usage -= int64(prevSize)
	c.usage += int64(newSize)

	c.sortedIndex = append(c.sortedIndex, &currentItem)
	c.index[currentItem.sha512Hex] = currentItem
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
	c.usage -= int64(currentItem.Size())
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
			c.usage -= int64(indexItem.Size())
			delete(c.index, indexItem.sha512Hex)
			i--

			continue
		}

		indexItem.sortIndex = currentIndex
		c.index[indexItem.sha512Hex] = *indexItem

		currentIndex++
	}
}

func (c *container) trimUnsafe(size int64) {
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

		dataSize := int64(indexItem.Size())

		c.sortedIndex[i] = nil
		c.usage -= dataSize
		delete(c.index, indexItem.sha512Hex)

		size -= dataSize
	}
}
