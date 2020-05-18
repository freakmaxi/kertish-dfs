package cluster

import (
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net"
	"strings"
	"time"
)

const commandCreate = "CREA"
const commandRead = "READ"
const commandDelete = "DELE"
const commandHardwareId = "HWID"
const commandJoin = "JOIN"
const commandMode = "MODE"
const commandLeave = "LEAV"
const commandWipe = "WIPE"
const commandSyncCreate = "SYCR"
const commandSyncDelete = "SYDE"
const commandSyncList = "SYLS"
const commandSyncFull = "SYFL"
const commandPing = "PING"
const commandSize = "SIZE"
const commandUsed = "USED"

type DataNode interface {
	Create(data []byte) (string, error)
	Read(sha512Hex string, readHandler func(data []byte) error) error
	Delete(sha512Hex string) error

	HardwareId() (string, error)
	Join(clusterId string, nodeId string, masterAddress string) bool
	Mode(master bool) bool
	Leave() bool
	Wipe() bool

	SyncCreate(sourceNodeAddr string, sha512Hex string) bool
	SyncDelete(sha512Hex string) bool
	SyncList() []string
	SyncFull(sourceNodeAddr string) bool

	Ping() int64
	Size() (uint64, error)
	Used() (uint64, error)

	Clone() DataNode
}

type dataNode struct {
	address *net.TCPAddr
	sha512  hash.Hash

	conn *net.TCPConn
}

func NewDataNode(nodeAddress string) (DataNode, error) {
	addr, err := net.ResolveTCPAddr("tcp", nodeAddress)
	if err != nil {
		return nil, err
	}

	return &dataNode{
		address: addr,
		sha512:  sha512.New512_256(),
	}, nil
}

func (d *dataNode) connect() error {
	var err error
	d.conn, err = net.DialTCP("tcp", nil, d.address)
	if err != nil {
		return err
	}
	return nil
}

func (d *dataNode) close() {
	_ = d.conn.Close()
}

func (d *dataNode) result() bool {
	b := make([]byte, 1)
	_, err := d.conn.Read(b)
	if err != nil {
		return false
	}

	return strings.Compare("+", string(b)) == 0
}

func (d *dataNode) resultWithTimeout(timeout time.Duration) bool {
	if timeout == 0 {
		timeout = time.Second * 30
	}

	if err := d.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return false
	}

	b := make([]byte, 1)
	_, err := d.conn.Read(b)
	if err != nil {
		return false
	}

	return strings.Compare("+", string(b)) == 0
}

func (d *dataNode) hashAsHex() (string, error) {
	h := make([]byte, 32)
	total, err := io.ReadAtLeast(d.conn, h, len(h))
	if err != nil {
		return "", err
	}
	if total != 32 {
		return "", fmt.Errorf("hash size is not 32 bytes length")
	}
	return hex.EncodeToString(h), nil
}

func (d *dataNode) Create(data []byte) (string, error) {
	d.sha512.Reset()

	if err := d.connect(); err != nil {
		return "", err
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandCreate)); err != nil {
		return "", err
	}

	d.sha512.Write(data)
	sha512Sum := d.sha512.Sum(nil)
	if _, err := d.conn.Write(sha512Sum); err != nil {
		return "", err
	}

	if !d.result() {
		return hex.EncodeToString(sha512Sum), nil
	}

	blockSize := uint32(len(data))
	if err := binary.Write(d.conn, binary.LittleEndian, &blockSize); err != nil {
		return "", err
	}

	if _, err := d.conn.Write(data); err != nil {
		return "", err
	}

	if !d.result() {
		return "", fmt.Errorf("create command is failed on data node")
	}

	return hex.EncodeToString(sha512Sum), nil
}

func (d *dataNode) Read(sha512Hex string, readHandler func([]byte) error) error {
	d.sha512.Reset()

	if err := d.connect(); err != nil {
		return err
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandRead)); err != nil {
		return err
	}

	sha512Sum, err := hex.DecodeString(sha512Hex)
	if err != nil {
		return err
	}
	if _, err := d.conn.Write(sha512Sum); err != nil {
		return err
	}

	if !d.result() {
		return fmt.Errorf("data node refused the read request")
	}

	var blockSize uint32
	if err := binary.Read(d.conn, binary.LittleEndian, &blockSize); err != nil {
		return err
	}

	readBuffer := make([]byte, blockSize)
	if _, err := io.ReadAtLeast(d.conn, readBuffer, len(readBuffer)); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}

	d.sha512.Write(readBuffer)
	if err := readHandler(readBuffer); err != nil {
		return err
	}

	if !d.result() {
		return fmt.Errorf("read command is failed on data node")
	}

	sha512HexCompare := hex.EncodeToString(d.sha512.Sum(nil))
	if strings.Compare(sha512Hex, sha512HexCompare) != 0 {
		return fmt.Errorf("read result is not verified")
	}

	return nil
}

func (d *dataNode) Delete(sha512Hex string) error {
	if err := d.connect(); err != nil {
		return err
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandDelete)); err != nil {
		return err
	}

	sha512Sum, err := hex.DecodeString(sha512Hex)
	if err != nil {
		return err
	}
	if _, err := d.conn.Write(sha512Sum); err != nil {
		return err
	}

	if !d.result() {
		return fmt.Errorf("delete command is failed on data node")
	}

	return nil
}

func (d *dataNode) HardwareId() (string, error) {
	if err := d.connect(); err != nil {
		return "", err
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandHardwareId)); err != nil {
		return "", err
	}

	if !d.result() {
		return "", fmt.Errorf("data node refused the hardware id request")
	}

	var hardwareIdLength byte
	if err := binary.Read(d.conn, binary.LittleEndian, &hardwareIdLength); err != nil {
		return "", err
	}

	readBuffer := make([]byte, hardwareIdLength)
	if _, err := io.ReadAtLeast(d.conn, readBuffer, len(readBuffer)); err != nil {
		return "", err
	}

	if !d.result() {
		return "", fmt.Errorf("hardware id command is failed on data node")
	}

	return string(readBuffer), nil
}

func (d *dataNode) Join(clusterId string, nodeId string, masterAddress string) bool {
	if err := d.connect(); err != nil {
		return false
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandJoin)); err != nil {
		return false
	}

	clusterIdLength := uint8(len(clusterId))
	if err := binary.Write(d.conn, binary.LittleEndian, clusterIdLength); err != nil {
		return false
	}

	if _, err := d.conn.Write([]byte(clusterId)); err != nil {
		return false
	}

	nodeIdLength := uint8(len(nodeId))
	if err := binary.Write(d.conn, binary.LittleEndian, nodeIdLength); err != nil {
		return false
	}

	if _, err := d.conn.Write([]byte(nodeId)); err != nil {
		return false
	}

	masterAddrLength := uint8(len(masterAddress))
	if err := binary.Write(d.conn, binary.LittleEndian, masterAddrLength); err != nil {
		return false
	}

	if _, err := d.conn.Write([]byte(masterAddress)); err != nil {
		return false
	}

	return d.result()
}

func (d *dataNode) Mode(master bool) bool {
	if err := d.connect(); err != nil {
		return false
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandMode)); err != nil {
		return false
	}

	if err := binary.Write(d.conn, binary.LittleEndian, master); err != nil {
		return false
	}

	return d.result()
}

func (d *dataNode) Leave() bool {
	if err := d.connect(); err != nil {
		return false
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandLeave)); err != nil {
		return false
	}

	return d.result()
}

//TODO: wipe security mechanism should be implemented between manager and data node
func (d *dataNode) Wipe() bool {
	if err := d.connect(); err != nil {
		return false
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandWipe)); err != nil {
		return false
	}

	// send wipe code in here
	//if err := binary.Write(d.conn, binary.LittleEndian, master); err != nil {
	//	return false
	//}

	return d.result()
}

func (d *dataNode) SyncCreate(sourceNodeAddr string, sha512Hex string) bool {
	sha512Sum, err := hex.DecodeString(sha512Hex)
	if err != nil {
		return false
	}

	if err := d.connect(); err != nil {
		return false
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandSyncCreate)); err != nil {
		return false
	}

	sourceBindAddrLength := uint8(len(sourceNodeAddr))
	if err := binary.Write(d.conn, binary.LittleEndian, sourceBindAddrLength); err != nil {
		return false
	}

	if _, err := d.conn.Write([]byte(sourceNodeAddr)); err != nil {
		return false
	}

	if _, err := d.conn.Write(sha512Sum); err != nil {
		return false
	}

	return d.result()
}

func (d *dataNode) SyncDelete(sha512Hex string) bool {
	sha512Sum, err := hex.DecodeString(sha512Hex)
	if err != nil {
		return false
	}

	if err := d.connect(); err != nil {
		return false
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandSyncDelete)); err != nil {
		return false
	}

	if _, err := d.conn.Write(sha512Sum); err != nil {
		return false
	}

	return d.result()
}

func (d *dataNode) SyncList() []string {
	if err := d.connect(); err != nil {
		return nil
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandSyncList)); err != nil {
		return nil
	}

	if !d.result() {
		return nil
	}

	var sha512HexListLength uint64
	if err := binary.Read(d.conn, binary.LittleEndian, &sha512HexListLength); err != nil {
		return nil
	}

	sha512HexList := make([]string, sha512HexListLength)
	for current := uint64(1); current <= sha512HexListLength; current++ {
		sha512Hex, err := d.hashAsHex()
		if err != nil {
			return nil
		}
		sha512HexList[current-1] = sha512Hex
	}

	if !d.result() {
		return nil
	}

	return sha512HexList
}

func (d *dataNode) SyncFull(sourceNodeAddr string) bool {
	if err := d.connect(); err != nil {
		return false
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandSyncFull)); err != nil {
		return false
	}

	sourceBindAddrLength := uint8(len(sourceNodeAddr))
	if err := binary.Write(d.conn, binary.LittleEndian, sourceBindAddrLength); err != nil {
		return false
	}

	if _, err := d.conn.Write([]byte(sourceNodeAddr)); err != nil {
		return false
	}

	return d.result()
}

func (d *dataNode) Ping() int64 {
	starts := time.Now().UTC()

	if err := d.connect(); err != nil {
		return -1
	}
	defer d.close()

	if err := d.conn.SetDeadline(time.Now().Add(time.Second * 5)); err != nil {
		return -1
	}

	if _, err := d.conn.Write([]byte(commandPing)); err != nil {
		return -1
	}

	if !d.resultWithTimeout(time.Second * 5) {
		return -1
	}

	return time.Now().UTC().Sub(starts).Milliseconds()
}

func (d *dataNode) Size() (uint64, error) {
	if err := d.connect(); err != nil {
		return 0, err
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandSize)); err != nil {
		return 0, err
	}

	if !d.result() {
		return 0, fmt.Errorf("data node refused the size request")
	}

	var size uint64
	if err := binary.Read(d.conn, binary.LittleEndian, &size); err != nil {
		return 0, err
	}

	if !d.result() {
		return 0, fmt.Errorf("size command is failed on data node")
	}

	return size, nil
}

func (d *dataNode) Used() (uint64, error) {
	if err := d.connect(); err != nil {
		return 0, err
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandUsed)); err != nil {
		return 0, err
	}

	if !d.result() {
		return 0, fmt.Errorf("data node refused the used request")
	}

	var used uint64
	if err := binary.Read(d.conn, binary.LittleEndian, &used); err != nil {
		return 0, err
	}

	if !d.result() {
		return 0, fmt.Errorf("used command is failed on data node")
	}

	return used, nil
}

func (d *dataNode) Clone() DataNode {
	return &dataNode{
		address: d.address,
		sha512:  sha512.New512_256(),
	}
}

var _ DataNode = &dataNode{}
