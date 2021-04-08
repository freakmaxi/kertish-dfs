package data

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/mediocregopher/radix/v3"
	"go.uber.org/zap"
)

const bulkOperationLimit = 5000
const semaphoreLimit = 10

const updatedAtKey = "__UPDATEDAT__"
const commandExecutorWaitDuration = time.Millisecond * 50

type Index interface {
	WaitQueueCompletion()

	QueueUpsert(item *common.CacheFileItem, syncTime *time.Time)
	QueueDrop(clusterId string, sha512Hex string)
	QueueUpsertChunkNode(sha512Hex string, nodeId string)
	QueueUpsertUsageInMap(clusterId string, items common.SyncFileItemList)

	Get(sha512Hex string) (*common.CacheFileItem, error)

	PullMap(clusterId string) (map[string]string, error)
	CompareMap(clusterId string, items common.SyncFileItemMap) bool
}

type index struct {
	logger    *zap.Logger
	client    CacheClient
	keyPrefix string

	commandChan         chan radix.CmdAction
	queueCompletionCond *sync.Cond
}

type keySuffix string

var (
	ksChunk      keySuffix = "chunk"
	ksChunkNodes keySuffix = "chunk_nodes"
	ksCluster    keySuffix = "cluster"
)

func NewIndex(client CacheClient, keyPrefix string, logger *zap.Logger) Index {
	i := &index{
		client:              client,
		keyPrefix:           keyPrefix,
		logger:              logger,
		commandChan:         make(chan radix.CmdAction, bulkOperationLimit*semaphoreLimit),
		queueCompletionCond: sync.NewCond(&sync.Mutex{}),
	}
	go i.commandExecutor()

	return i
}

func (i *index) commandExecutor() {
	commandSlotsMap := make(map[uint16][]radix.CmdAction)
	count := 0

	for {
		select {
		case command := <-i.commandChan:
			for _, key := range command.Keys() {
				slot := radix.ClusterSlot([]byte(key))

				if _, has := commandSlotsMap[slot]; !has {
					commandSlotsMap[slot] = make([]radix.CmdAction, 0)
				}

				commandSlotsMap[slot] = append(commandSlotsMap[slot], command)
				count++

				if count < bulkOperationLimit {
					continue
				}
				count = i.executeCommandSlots(commandSlotsMap)
			}
		case <-time.After(commandExecutorWaitDuration):
			if count == 0 {
				i.queueCompletionCond.Broadcast()
				continue
			}
			count = i.executeCommandSlots(commandSlotsMap)
		}
	}
}

func (i *index) executeCommandSlots(commandSlotsMap map[uint16][]radix.CmdAction) int {
	commandSlotsMapMutex := sync.Mutex{}
	getSlotFunc := func(slot uint16) []radix.CmdAction {
		commandSlotsMapMutex.Lock()
		defer commandSlotsMapMutex.Unlock()

		return commandSlotsMap[slot]
	}
	deleteSlotFunc := func(slot uint16) {
		commandSlotsMapMutex.Lock()
		defer commandSlotsMapMutex.Unlock()

		delete(commandSlotsMap, slot)
	}

	semaphoreChan := make(chan bool, semaphoreLimit)
	for i := 0; i < cap(semaphoreChan); i++ {
		semaphoreChan <- true
	}
	defer close(semaphoreChan)

	executionFunc := func(wg *sync.WaitGroup, slot uint16, commandSlotsMap map[uint16][]radix.CmdAction) {
		defer wg.Done()
		defer func() { semaphoreChan <- true }()

		slotCommands := getSlotFunc(slot)
		if err := i.client.Pipeline(slotCommands); err != nil {
			i.logger.Warn("Execution of cache pipeline is failed", zap.Error(err))
			return
		}
		deleteSlotFunc(slot)
	}

	slotCache := make([]uint16, 0)
	for slot := range commandSlotsMap {
		slotCache = append(slotCache, slot)
	}

	wg := &sync.WaitGroup{}
	for _, slot := range slotCache {
		wg.Add(1)
		go executionFunc(wg, slot, commandSlotsMap)

		<-semaphoreChan
	}
	wg.Wait()

	count := 0
	for _, c := range commandSlotsMap {
		count += len(c)
	}
	return count
}

func (i *index) WaitQueueCompletion() {
	i.queueCompletionCond.L.Lock()
	defer i.queueCompletionCond.L.Unlock()

	i.queueCompletionCond.Wait()
	i.logger.Info("Execution of cache pipeline is completed")
}

func (i *index) key(name string, suffix keySuffix) string {
	key := fmt.Sprintf("%s_index_%s", i.keyPrefix, name)
	if len(suffix) == 0 {
		return key
	}
	return fmt.Sprintf("%s_%s", key, suffix)
}

func (i *index) replaceCommand(item *common.CacheFileItem, syncTime *time.Time) []radix.CmdAction {
	chunkKey := i.key(item.FileItem.Sha512Hex, ksChunk)
	chunkNodesKey := i.key(item.FileItem.Sha512Hex, ksChunkNodes)
	clusterKey := i.key(item.ClusterId, ksCluster)

	commands := make([]radix.CmdAction, 0)
	currentTime := time.Now().UTC().Format(time.RFC3339)

	commands = append(commands, radix.Cmd(nil, "HSET", chunkKey, updatedAtKey, currentTime))
	for k, v := range item.Export() {
		commands = append(commands, radix.Cmd(nil, "HSET", chunkKey, k, v))
	}
	commands = append(commands,
		radix.Cmd(nil, "EXPIREAT", chunkKey, strconv.FormatInt(item.ExpiresAt.Unix(), 10)))

	commands = append(commands, radix.Cmd(nil, "HSET", chunkNodesKey, updatedAtKey, currentTime))
	for nodeId := range item.ExistsIn {
		commands = append(commands, radix.Cmd(nil, "HSET", chunkNodesKey, nodeId, currentTime))
	}
	commands = append(commands,
		radix.Cmd(nil, "EXPIREAT", chunkNodesKey, strconv.FormatInt(item.ExpiresAt.Unix(), 10)))

	if syncTime != nil {
		commands = append(commands,
			radix.Cmd(nil, "HSET", clusterKey, updatedAtKey, syncTime.Format(time.RFC3339)))
	}
	commands = append(commands,
		radix.Cmd(nil, "HSET", clusterKey, item.FileItem.Sha512Hex,
			fmt.Sprintf("%d|%s", item.FileItem.Usage, currentTime),
		))

	return commands
}

func (i *index) dropCommand(clusterId string, sha512Hex string) []radix.CmdAction {
	commands := make([]radix.CmdAction, 0)
	commands = append(commands,
		radix.Cmd(nil, "DEL", i.key(sha512Hex, ksChunk)))
	commands = append(commands,
		radix.Cmd(nil, "DEL", i.key(sha512Hex, ksChunkNodes)))
	commands = append(commands,
		radix.Cmd(nil, "HDEL", i.key(clusterId, ksCluster), sha512Hex))

	return commands
}

func (i *index) QueueUpsert(item *common.CacheFileItem, syncTime *time.Time) {
	if item == nil {
		return
	}

	for _, c := range i.replaceCommand(item, syncTime) {
		i.commandChan <- c
	}
}

func (i *index) QueueDrop(clusterId string, sha512Hex string) {
	if len(sha512Hex) == 0 {
		return
	}

	for _, c := range i.dropCommand(clusterId, sha512Hex) {
		i.commandChan <- c
	}
}

func (i *index) QueueUpsertChunkNode(sha512Hex string, nodeId string) {
	if len(sha512Hex) == 0 {
		return
	}

	i.commandChan <- radix.Cmd(nil, "HSET", i.key(sha512Hex, ksChunkNodes), nodeId,
		time.Now().UTC().Format(time.RFC3339))
}

func (i *index) QueueUpsertUsageInMap(clusterId string, fileItemList common.SyncFileItemList) {
	if len(fileItemList) == 0 {
		return
	}

	clusterKey := i.key(clusterId, ksCluster)
	currentTime := time.Now().UTC().Format(time.RFC3339)

	for _, fileItem := range fileItemList {
		chunkKey := i.key(fileItem.Sha512Hex, ksChunk)

		i.commandChan <- radix.Cmd(nil, "HSET", chunkKey, "usage",
			strconv.FormatUint(uint64(fileItem.Usage), 10))
		i.commandChan <- radix.Cmd(nil, "HSET", clusterKey, fileItem.Sha512Hex,
			fmt.Sprintf("%d|%s", fileItem.Usage, currentTime))
	}
}

func (i *index) Get(sha512Hex string) (*common.CacheFileItem, error) {
	chunkKey := i.key(sha512Hex, ksChunk)

	c, err := i.client.HGetAll(chunkKey)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, os.ErrNotExist
	}

	item := common.NewCacheFileItemFromMap(c)
	if item.Expired() {
		return nil, os.ErrNotExist
	}

	hashUpdatedAtValue, has := c[updatedAtKey]
	if !has {
		return nil, os.ErrNotExist
	}

	hashUpdatedAt, err := time.Parse(time.RFC3339, hashUpdatedAtValue)
	if err != nil {
		return nil, err
	}

	chunkNodeKey := i.key(sha512Hex, ksChunkNodes)
	cn, err := i.client.HGetAll(chunkNodeKey)
	if err != nil {
		return nil, err
	}

	for nodeId, updateAtValue := range cn {
		updatedAt, err := time.Parse(time.RFC3339, updateAtValue)
		if err != nil {
			continue
		}
		if updatedAt.Before(hashUpdatedAt) {
			continue
		}
		item.ExistsIn[nodeId] = true
	}

	return item, nil
}

func (i *index) pullMap(clusterId string) (map[string]string, error) {
	return i.client.HGetAll(i.key(clusterId, ksCluster))
}

func (i *index) PullMap(clusterId string) (map[string]string, error) {
	cachedMap, err := i.pullMap(clusterId)
	if err != nil {
		return nil, err
	}
	delete(cachedMap, updatedAtKey)

	return cachedMap, nil
}

func (i *index) CompareMap(clusterId string, items common.SyncFileItemMap) bool {
	cachedMap, err := i.pullMap(clusterId)
	if err != nil {
		return false
	}

	hashUpdatedAtValue, has := cachedMap[updatedAtKey]
	if !has {
		return false
	}

	delete(cachedMap, updatedAtKey)

	hashUpdatedAt, err := time.Parse(time.RFC3339, hashUpdatedAtValue)
	if err != nil {
		return false
	}

	for sha512Hex, itemValue := range cachedMap {
		fileItem, has := items[sha512Hex]
		if !has {
			return false
		}

		pipeIdx := strings.Index(itemValue, "|")
		if pipeIdx == -1 {
			return false
		}

		// Check if it is an old entry
		updateAt, err := time.Parse(time.RFC3339, itemValue[pipeIdx+1:])
		if err != nil {
			return false
		}
		if hashUpdatedAt.After(updateAt) {
			return false
		}

		usage, err := strconv.ParseUint(itemValue[:pipeIdx], 10, 16)
		if err != nil {
			return false
		}

		if fileItem.Usage != uint16(usage) {
			return false
		}
	}

	return true
}

var _ Index = &index{}
