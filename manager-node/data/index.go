package data

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/locking-center-client-go/mutex"
	"github.com/mediocregopher/radix/v3"
)

const bulkOperationLimit = 5000
const semaphoreLimit = 10

type Index interface {
	ReplaceBulk(items common.CacheFileItemMap) error
	UpdateChunkNodeBulk(sha512HexList []string, nodeId string, exists bool) error
	DropBulk(clusterId string, sha512HexList []string) error

	Get(sha512Hex string) (*common.CacheFileItem, error)
	Replace(item common.CacheFileItem) error
	UpdateChunkNode(sha512Hex string, nodeId string, exists bool) error
	Drop(clusterId string, sha512Hex string) error

	PutMap(clusterId string, items common.SyncFileItemMap) error
	UpdateUsageInMap(clusterId string, items common.SyncFileItemList) error
	PullMap(clusterId string) (map[string]string, error)
	CompareMap(clusterId string, items common.SyncFileItemMap) bool
	DropMap(clusterId string) error
}

type index struct {
	mutex     mutex.LockingCenter
	client    CacheClient
	keyPrefix string
}

type keySuffix string

var (
	ksEmpty      keySuffix = ""
	ksChunk      keySuffix = "chunk"
	ksChunkNodes keySuffix = "chunk_nodes"
	ksCluster    keySuffix = "cluster"
)

func NewIndex(mutex mutex.LockingCenter, client CacheClient, keyPrefix string) Index {
	return &index{
		mutex:     mutex,
		client:    client,
		keyPrefix: keyPrefix,
	}
}

func (i *index) key(name string, suffix keySuffix) string {
	key := fmt.Sprintf("%s_index_%s", i.keyPrefix, name)
	if len(suffix) == 0 {
		return key
	}
	return fmt.Sprintf("%s_%s", key, suffix)
}

func (i *index) replaceCommand(item *common.CacheFileItem) []radix.CmdAction {
	chunkKey := i.key(item.FileItem.Sha512Hex, ksChunk)
	chunkNodesKey := i.key(item.FileItem.Sha512Hex, ksChunkNodes)
	clusterKey := i.key(item.ClusterId, ksCluster)

	commands := make([]radix.CmdAction, 0)

	for k, v := range item.Export() {
		commands = append(commands, radix.Cmd(nil, "HSET", chunkKey, k, v))
	}
	commands = append(commands,
		radix.Cmd(nil, "EXPIREAT", chunkKey, strconv.FormatInt(item.ExpiresAt.Unix(), 10)))

	for k, v := range item.ExistsIn {
		commands = append(commands, radix.Cmd(nil, "HSET", chunkNodesKey, k, strconv.FormatBool(v)))
	}
	commands = append(commands,
		radix.Cmd(nil, "EXPIREAT", chunkNodesKey, strconv.FormatInt(item.ExpiresAt.Unix(), 10)))

	commands = append(commands,
		radix.Cmd(nil, "HSET", clusterKey, item.FileItem.Sha512Hex, fmt.Sprintf("%s|%d", item.ClusterId, item.FileItem.Usage)))

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

func (i *index) mapToSlots(commands []radix.CmdAction, commandSlotsMap map[uint16][]radix.CmdAction) int {
	count := 0

	for _, command := range commands {
		for _, key := range command.Keys() {
			slot := radix.ClusterSlot([]byte(key))

			if _, has := commandSlotsMap[slot]; !has {
				commandSlotsMap[slot] = make([]radix.CmdAction, 0)
			}

			commandSlotsMap[slot] = append(commandSlotsMap[slot], command)
			count++
		}
	}

	return count
}

func (i *index) executeCommandSlots(commandSlotsMap map[uint16][]radix.CmdAction) (resultErr error) {
	errorsMutex := sync.Mutex{}
	appendToErrorsFunc := func(err error) {
		errorsMutex.Lock()
		defer errorsMutex.Unlock()

		if resultErr == nil {
			resultErr = errors.NewBulkError()
		}
		resultErr.(*errors.BulkError).Add(err)
	}

	semaphoreChan := make(chan bool, semaphoreLimit)
	for i := 0; i < cap(semaphoreChan); i++ {
		semaphoreChan <- true
	}
	defer close(semaphoreChan)

	executionFunc := func(wg *sync.WaitGroup, slotCommands []radix.CmdAction) {
		defer wg.Done()
		defer func() { semaphoreChan <- true }()

		if err := i.client.Pipeline(slotCommands); err != nil {
			appendToErrorsFunc(err)
		}
	}

	wg := &sync.WaitGroup{}
	for _, slotCommands := range commandSlotsMap {
		wg.Add(1)
		go executionFunc(wg, slotCommands)

		<-semaphoreChan
	}
	wg.Wait()

	return
}

func (i *index) DropBulk(clusterId string, sha512HexList []string) error {
	if len(sha512HexList) == 0 {
		return nil
	}

	key := i.key("bulk", ksEmpty)

	i.mutex.Lock(key)
	defer i.mutex.Unlock(key)

	count := 0
	commandSlotsMap := make(map[uint16][]radix.CmdAction)

	for _, sha512Hex := range sha512HexList {
		dropCommands := i.dropCommand(clusterId, sha512Hex)
		count += i.mapToSlots(dropCommands, commandSlotsMap)

		if count >= bulkOperationLimit {
			if err := i.executeCommandSlots(commandSlotsMap); err != nil {
				return err
			}

			count = 0
			commandSlotsMap = make(map[uint16][]radix.CmdAction)
		}
	}

	return i.executeCommandSlots(commandSlotsMap)
}

func (i *index) Replace(item common.CacheFileItem) error {
	i.mutex.Wait(i.key("bulk", ksEmpty))

	key := i.key(item.FileItem.Sha512Hex, ksEmpty)

	i.mutex.Lock(key)
	defer i.mutex.Unlock(key)

	commandSlotsMap := make(map[uint16][]radix.CmdAction)

	replaceCommands := i.replaceCommand(&item)
	_ = i.mapToSlots(replaceCommands, commandSlotsMap)

	return i.executeCommandSlots(commandSlotsMap)
}

func (i *index) ReplaceBulk(items common.CacheFileItemMap) error {
	if len(items) == 0 {
		return nil
	}

	key := i.key("bulk", ksEmpty)

	i.mutex.Lock(key)
	defer i.mutex.Unlock(key)

	count := 0
	commandSlotMap := make(map[uint16][]radix.CmdAction)

	for _, item := range items {
		commands := i.replaceCommand(item)
		count += i.mapToSlots(commands, commandSlotMap)

		if count >= bulkOperationLimit {
			if err := i.executeCommandSlots(commandSlotMap); err != nil {
				return err
			}

			count = 0
			commandSlotMap = make(map[uint16][]radix.CmdAction)
		}
	}

	return i.executeCommandSlots(commandSlotMap)
}

func (i *index) UpdateChunkNode(sha512Hex string, nodeId string, exists bool) error {
	i.mutex.Wait(i.key("bulk", ksEmpty))

	key := i.key(sha512Hex, ksEmpty)

	i.mutex.Lock(key)
	defer i.mutex.Unlock(key)

	return i.client.Do(
		radix.Cmd(nil, "HSET",
			i.key(sha512Hex, ksChunkNodes),
			nodeId,
			strconv.FormatBool(exists),
		),
	)
}

func (i *index) UpdateChunkNodeBulk(sha512HexList []string, nodeId string, exists bool) error {
	if len(sha512HexList) == 0 {
		return nil
	}

	key := i.key("bulk", ksEmpty)

	i.mutex.Lock(key)
	defer i.mutex.Unlock(key)

	count := 0
	commandSlotMap := make(map[uint16][]radix.CmdAction)

	for _, sha512Hex := range sha512HexList {
		count += i.mapToSlots(
			[]radix.CmdAction{
				radix.Cmd(nil, "HSET",
					i.key(sha512Hex, ksChunkNodes),
					nodeId,
					strconv.FormatBool(exists),
				),
			},
			commandSlotMap,
		)

		if count >= bulkOperationLimit {
			if err := i.executeCommandSlots(commandSlotMap); err != nil {
				return err
			}

			count = 0
			commandSlotMap = make(map[uint16][]radix.CmdAction)
		}
	}

	return i.executeCommandSlots(commandSlotMap)
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

	chunkNodeKey := i.key(sha512Hex, ksChunkNodes)
	cn, err := i.client.HGetAll(chunkNodeKey)
	if err != nil {
		return nil, err
	}

	for k, v := range cn {
		nodeState, err := strconv.ParseBool(v)
		if err != nil {
			nodeState = false
		}
		item.ExistsIn[k] = nodeState
	}

	return item, nil
}

func (i *index) Drop(clusterId string, sha512Hex string) error {
	i.mutex.Wait(i.key("bulk", ksEmpty))

	key := i.key(sha512Hex, ksEmpty)

	i.mutex.Lock(key)
	defer i.mutex.Unlock(key)

	commandSlotsMap := make(map[uint16][]radix.CmdAction)

	dropCommands := i.dropCommand(clusterId, sha512Hex)
	_ = i.mapToSlots(dropCommands, commandSlotsMap)

	return i.executeCommandSlots(commandSlotsMap)
}

func (i *index) PutMap(clusterId string, items common.SyncFileItemMap) error {
	sha512HexMap := make(map[string]string)

	for _, item := range items {
		sha512HexMap[item.Sha512Hex] = fmt.Sprintf("%s|%d", clusterId, item.Usage)
	}

	return i.client.HMSet(i.key(clusterId, ksCluster), sha512HexMap)
}

func (i *index) UpdateUsageInMap(clusterId string, fileItemList common.SyncFileItemList) error {
	if len(fileItemList) == 0 {
		return nil
	}

	key := i.key("bulk", ksEmpty)

	i.mutex.Lock(key)
	defer i.mutex.Unlock(key)

	clusterKey := i.key(clusterId, ksCluster)

	count := 0
	commandSlotMap := make(map[uint16][]radix.CmdAction)

	for _, fileItem := range fileItemList {
		chunkKey := i.key(fileItem.Sha512Hex, ksChunk)

		count += i.mapToSlots(
			[]radix.CmdAction{
				radix.Cmd(nil, "HSET",
					chunkKey,
					"usage",
					strconv.FormatUint(uint64(fileItem.Usage), 10),
				),
				radix.Cmd(nil, "HSET",
					clusterKey,
					fileItem.Sha512Hex,
					fmt.Sprintf("%s|%d", clusterId, fileItem.Usage),
				),
			},
			commandSlotMap,
		)

		if count >= bulkOperationLimit {
			if err := i.executeCommandSlots(commandSlotMap); err != nil {
				return err
			}

			count = 0
			commandSlotMap = make(map[uint16][]radix.CmdAction)
		}
	}

	return i.executeCommandSlots(commandSlotMap)
}

func (i *index) PullMap(clusterId string) (map[string]string, error) {
	return i.client.HGetAll(i.key(clusterId, ksCluster))
}

func (i *index) CompareMap(clusterId string, items common.SyncFileItemMap) bool {
	cachedMap, err := i.PullMap(clusterId)
	if err != nil {
		return false
	}

	for sha512Hex, itemValue := range cachedMap {
		fileItem, has := items[sha512Hex]
		if !has {
			return false
		}
		compareValue := fmt.Sprintf("%s|%d", clusterId, fileItem.Usage)
		if strings.Compare(itemValue, compareValue) != 0 {
			return false
		}
	}

	return true
}

func (i *index) DropMap(clusterId string) error {
	return i.client.Del(i.key(clusterId, ksCluster))
}

var _ Index = &index{}
