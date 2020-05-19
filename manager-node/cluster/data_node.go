package cluster

import (
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
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
const commandSyncMove = "SYMV"
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
	SyncMove(sourceNodeAddr string, sha512Hex string) bool
	SyncList() []string
	SyncFull(sourceNodeAddr string) bool

	Ping() int64
	Size() (uint64, error)
	Used() (uint64, error)
}

type dataNode struct {
	address *net.TCPAddr
}

func NewDataNode(nodeAddress string) (DataNode, error) {
	addr, err := net.ResolveTCPAddr("tcp", nodeAddress)
	if err != nil {
		return nil, err
	}

	return &dataNode{
		address: addr,
	}, nil
}

func (d *dataNode) connect(connectionHandler func(conn *net.TCPConn) error) error {
	conn, err := net.DialTCP("tcp", nil, d.address)
	if err != nil {
		return err
	}
	defer conn.Close()

	return connectionHandler(conn)
}

func (d *dataNode) result(conn *net.TCPConn) bool {
	b := make([]byte, 1)
	_, err := conn.Read(b)
	if err != nil {
		return false
	}
	return strings.Compare("+", string(b)) == 0
}

func (d *dataNode) resultWithTimeout(conn *net.TCPConn, timeout time.Duration) bool {
	if timeout == 0 {
		timeout = time.Second * 30
	}

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return false
	}
	return d.result(conn)
}

func (d *dataNode) hashAsHex(conn *net.TCPConn) (string, error) {
	h := make([]byte, 32)
	total, err := io.ReadAtLeast(conn, h, len(h))
	if err != nil {
		return "", err
	}
	if total != 32 {
		return "", fmt.Errorf("hash size is not 32 bytes length")
	}
	return hex.EncodeToString(h), nil
}

func (d *dataNode) Create(data []byte) (sha512Hex string, err error) {
	err = d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandCreate)); err != nil {
			return err
		}

		sha512Hash := sha512.New512_256()
		sha512Hash.Write(data)

		sha512Sum := sha512Hash.Sum(nil)
		if _, err := conn.Write(sha512Sum); err != nil {
			return err
		}

		if !d.result(conn) {
			sha512Hex = hex.EncodeToString(sha512Sum)
			return nil
		}

		blockSize := uint32(len(data))
		if err := binary.Write(conn, binary.LittleEndian, &blockSize); err != nil {
			return err
		}

		if _, err := conn.Write(data); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("create command is failed on data node")
		}

		sha512Hex = hex.EncodeToString(sha512Sum)
		return nil
	})
	return
}

func (d *dataNode) Read(sha512Hex string, readHandler func([]byte) error) error {
	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandRead)); err != nil {
			return err
		}

		sha512Sum, err := hex.DecodeString(sha512Hex)
		if err != nil {
			return err
		}
		if _, err := conn.Write(sha512Sum); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("data node refused the read request")
		}

		var blockSize uint32
		if err := binary.Read(conn, binary.LittleEndian, &blockSize); err != nil {
			return err
		}

		readBuffer := make([]byte, blockSize)
		if _, err := io.ReadAtLeast(conn, readBuffer, len(readBuffer)); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		sha512Hash := sha512.New512_256()
		sha512Hash.Write(readBuffer)

		if err := readHandler(readBuffer); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("read command is failed on data node")
		}

		sha512HexCompare := hex.EncodeToString(sha512Hash.Sum(nil))
		if strings.Compare(sha512Hex, sha512HexCompare) != 0 {
			return fmt.Errorf("read result is not verified")
		}

		return nil
	})
}

func (d *dataNode) Delete(sha512Hex string) error {
	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandDelete)); err != nil {
			return err
		}

		sha512Sum, err := hex.DecodeString(sha512Hex)
		if err != nil {
			return err
		}
		if _, err := conn.Write(sha512Sum); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("delete command is failed on data node")
		}

		return nil
	})
}

func (d *dataNode) HardwareId() (hardwareId string, err error) {
	err = d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandHardwareId)); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("data node refused the hardware id request")
		}

		var hardwareIdLength byte
		if err := binary.Read(conn, binary.LittleEndian, &hardwareIdLength); err != nil {
			return err
		}

		readBuffer := make([]byte, hardwareIdLength)
		if _, err := io.ReadAtLeast(conn, readBuffer, len(readBuffer)); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("hardware id command is failed on data node")
		}

		hardwareId = string(readBuffer)
		return nil
	})
	return
}

func (d *dataNode) Join(clusterId string, nodeId string, masterAddress string) bool {
	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandJoin)); err != nil {
			return err
		}

		clusterIdLength := uint8(len(clusterId))
		if err := binary.Write(conn, binary.LittleEndian, clusterIdLength); err != nil {
			return err
		}

		if _, err := conn.Write([]byte(clusterId)); err != nil {
			return err
		}

		nodeIdLength := uint8(len(nodeId))
		if err := binary.Write(conn, binary.LittleEndian, nodeIdLength); err != nil {
			return err
		}

		if _, err := conn.Write([]byte(nodeId)); err != nil {
			return err
		}

		masterAddrLength := uint8(len(masterAddress))
		if err := binary.Write(conn, binary.LittleEndian, masterAddrLength); err != nil {
			return err
		}

		if _, err := conn.Write([]byte(masterAddress)); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("join command is failed on data node")
		}

		return nil
	}) == nil
}

func (d *dataNode) Mode(master bool) bool {
	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandMode)); err != nil {
			return err
		}

		if err := binary.Write(conn, binary.LittleEndian, master); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("mode command is failed on data node")
		}

		return nil
	}) == nil
}

func (d *dataNode) Leave() bool {
	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandLeave)); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("leave command is failed on data node")
		}

		return nil
	}) == nil
}

//TODO: wipe security mechanism should be implemented between manager and data node
func (d *dataNode) Wipe() bool {
	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandWipe)); err != nil {
			return err
		}

		// send wipe code in here
		//if err := binary.Write(d.conn, binary.LittleEndian, master); err != nil {
		//	return false
		//}

		if !d.result(conn) {
			return fmt.Errorf("wipe command is failed on data node")
		}

		return nil
	}) == nil
}

func (d *dataNode) SyncCreate(sourceNodeAddr string, sha512Hex string) bool {
	sha512Sum, err := hex.DecodeString(sha512Hex)
	if err != nil {
		return false
	}

	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandSyncCreate)); err != nil {
			return err
		}

		sourceBindAddrLength := uint8(len(sourceNodeAddr))
		if err := binary.Write(conn, binary.LittleEndian, sourceBindAddrLength); err != nil {
			return err
		}

		if _, err := conn.Write([]byte(sourceNodeAddr)); err != nil {
			return err
		}

		if _, err := conn.Write(sha512Sum); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("sync create command is failed on data node")
		}

		return nil
	}) == nil
}

func (d *dataNode) SyncDelete(sha512Hex string) bool {
	sha512Sum, err := hex.DecodeString(sha512Hex)
	if err != nil {
		return false
	}

	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandSyncDelete)); err != nil {
			return err
		}

		if _, err := conn.Write(sha512Sum); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("sync delete command is failed on data node")
		}

		return nil
	}) == nil
}

func (d *dataNode) SyncMove(sourceNodeAddr string, sha512Hex string) bool {
	sha512Sum, err := hex.DecodeString(sha512Hex)
	if err != nil {
		return false
	}

	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandSyncMove)); err != nil {
			return err
		}

		sourceBindAddrLength := uint8(len(sourceNodeAddr))
		if err := binary.Write(conn, binary.LittleEndian, sourceBindAddrLength); err != nil {
			return err
		}

		if _, err := conn.Write([]byte(sourceNodeAddr)); err != nil {
			return err
		}

		if _, err := conn.Write(sha512Sum); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("sync move command is failed on data node")
		}

		return nil
	}) == nil
}

func (d *dataNode) SyncList() (sha512HexList []string) {
	if err := d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandSyncList)); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("data node refused the sync list request")
		}

		var sha512HexListLength uint64
		if err := binary.Read(conn, binary.LittleEndian, &sha512HexListLength); err != nil {
			return err
		}

		sha512HexList = make([]string, sha512HexListLength)
		for current := uint64(1); current <= sha512HexListLength; current++ {
			sha512Hex, err := d.hashAsHex(conn)
			if err != nil {
				return err
			}
			sha512HexList[current-1] = sha512Hex
		}

		if !d.result(conn) {
			return fmt.Errorf("sync list command is failed on data node")
		}

		return nil
	}); err != nil {
		return nil
	}
	return
}

func (d *dataNode) SyncFull(sourceNodeAddr string) bool {
	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandSyncFull)); err != nil {
			return err
		}

		sourceBindAddrLength := uint8(len(sourceNodeAddr))
		if err := binary.Write(conn, binary.LittleEndian, sourceBindAddrLength); err != nil {
			return err
		}

		if _, err := conn.Write([]byte(sourceNodeAddr)); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("sync full command is failed on data node")
		}

		return nil
	}) == nil
}

func (d *dataNode) Ping() (latency int64) {
	starts := time.Now().UTC()

	if err := d.connect(func(conn *net.TCPConn) error {
		if err := conn.SetDeadline(time.Now().Add(time.Second * 5)); err != nil {
			return err
		}

		if _, err := conn.Write([]byte(commandPing)); err != nil {
			return err
		}

		if !d.resultWithTimeout(conn, time.Second*5) {
			return fmt.Errorf("ping command is failed on data node")
		}

		latency = time.Now().UTC().Sub(starts).Milliseconds()
		return nil
	}); err != nil {
		return -1
	}
	return
}

func (d *dataNode) Size() (size uint64, err error) {
	err = d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandSize)); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("data node refused the size request")
		}

		if err := binary.Read(conn, binary.LittleEndian, &size); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("size command is failed on data node")
		}

		return nil
	})
	return
}

func (d *dataNode) Used() (used uint64, err error) {
	err = d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandUsed)); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("data node refused the used request")
		}

		if err := binary.Read(conn, binary.LittleEndian, &used); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("used command is failed on data node")
		}

		return nil
	})
	return
}

var _ DataNode = &dataNode{}
