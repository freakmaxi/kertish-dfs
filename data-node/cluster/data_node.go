package cluster

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
)

const commandSyncRead = "SYRD"
const commandSyncList = "SYLS"
const chunkSize = 1024 * 1024 // 1mb

type DataNode interface {
	SyncList(snapshotTime *time.Time) (*common.SyncContainer, error)
	SyncRead(snapshotTime *time.Time, sha512Hex string, drop bool,
		dataHandler func(data []byte) error,
		verifyHandler func(usage uint16) bool,
	) error
}

type dataNode struct {
	address *net.TCPAddr
}

func NewDataNode(address string) (DataNode, error) {
	addr, err := net.ResolveTCPAddr("tcp", address)
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
	defer func() { _ = conn.Close() }()

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

func (d *dataNode) SyncList(snapshotTime *time.Time) (*common.SyncContainer, error) {
	container := common.NewSyncContainer()

	if err := d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandSyncList)); err != nil {
			return err
		}

		snapshotTimeUint := uint64(0)
		if snapshotTime != nil {
			snapshotTimeUint, _ = strconv.ParseUint(snapshotTime.Format(common.MachineTimeFormatWithSeconds), 10, 64)
		}

		if err := binary.Write(conn, binary.LittleEndian, snapshotTimeUint); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("data node refused the sync list request")
		}

		var snapshotsLength uint64
		if err := binary.Read(conn, binary.LittleEndian, &snapshotsLength); err != nil {
			return err
		}

		for i := uint64(0); i < snapshotsLength; i++ {
			var snapshotTimeUint uint64
			if err := binary.Read(conn, binary.LittleEndian, &snapshotTimeUint); err != nil {
				return err
			}

			snapshotTime, err := time.Parse(common.MachineTimeFormatWithSeconds, strconv.FormatUint(snapshotTimeUint, 10))
			if err != nil {
				return err
			}

			container.Snapshots = append(container.Snapshots, snapshotTime)
		}
		container.Sort()

		for {
			sha512Hex, err := d.hashAsHex(conn)
			if err != nil {
				return err
			}
			if strings.Compare(sha512Hex, common.NullSha512Hex) == 0 {
				break
			}

			var usage uint16
			if err := binary.Read(conn, binary.LittleEndian, &usage); err != nil {
				return err
			}

			var size int32
			if err := binary.Read(conn, binary.LittleEndian, &size); err != nil {
				return err
			}

			container.FileItems[sha512Hex] =
				common.SyncFileItem{
					Sha512Hex: sha512Hex,
					Usage:     usage,
					Size:      uint32(size),
				}
		}

		if !d.result(conn) {
			return fmt.Errorf("sync list command is failed on data node")
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return container, nil
}

func (d *dataNode) SyncRead(snapshotTime *time.Time, sha512Hex string, drop bool, dataHandler func([]byte) error, verifyHandler func(usage uint16) bool) error {
	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandSyncRead)); err != nil {
			return err
		}

		sha512Sum, err := hex.DecodeString(sha512Hex)
		if err != nil {
			return err
		}
		if _, err := conn.Write(sha512Sum); err != nil {
			return err
		}

		if err := binary.Write(conn, binary.LittleEndian, drop); err != nil {
			return err
		}

		snapshotTimeUint := uint64(0)
		if snapshotTime != nil {
			snapshotTimeUint, _ = strconv.ParseUint(snapshotTime.Format(common.MachineTimeFormatWithSeconds), 10, 64)
		}

		if err := binary.Write(conn, binary.LittleEndian, snapshotTimeUint); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("data node refused the sync read request")
		}

		var blockSize uint32
		if err := binary.Read(conn, binary.LittleEndian, &blockSize); err != nil {
			return err
		}

		var usage uint16
		if err := binary.Read(conn, binary.LittleEndian, &usage); err != nil {
			return err
		}

		if _, err := conn.Write([]byte{'+'}); err != nil {
			return err
		}

		readBuffer := make([]byte, chunkSize)
		if blockSize < chunkSize {
			readBuffer = make([]byte, blockSize)
		}
		for blockSize > 0 {
			_, err := io.ReadAtLeast(conn, readBuffer, len(readBuffer))
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			if err := dataHandler(readBuffer); err != nil {
				return err
			}

			blockSize -= uint32(len(readBuffer))
			if blockSize < chunkSize {
				readBuffer = make([]byte, blockSize)
			}
		}

		if !d.result(conn) {
			return fmt.Errorf("sync read command is failed on data node")
		}

		if !verifyHandler(usage) {
			return fmt.Errorf("sync read result is not verified")
		}

		return nil
	})
}

var _ DataNode = &dataNode{}
