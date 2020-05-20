package cluster

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
)

const commandSyncRead = "SYRD"
const commandSyncList = "SYLS"
const chunkSize = 1024 * 1024 // 1mb

type DataNode interface {
	SyncRead(sha512Hex string, drop bool, usageCountHandler func(usageCount uint16) bool, dataHandler func(data []byte) error, verifyHandler func() bool) error
	SyncList(readHandler func(fileItem common.SyncFileItem, current uint64, total uint64) error) error
}

type dataNode struct {
	address *net.TCPAddr

	conn *net.TCPConn
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

func (d *dataNode) SyncRead(sha512Hex string, drop bool, usageCountHandler func(usageCount uint16) bool, dataHandler func([]byte) error, verifyHandler func() bool) error {
	if err := d.connect(); err != nil {
		return err
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandSyncRead)); err != nil {
		return err
	}

	if err := binary.Write(d.conn, binary.LittleEndian, drop); err != nil {
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

	usageCountBuffer := make([]byte, 2)
	if _, err := io.ReadAtLeast(d.conn, usageCountBuffer, len(usageCountBuffer)); err != nil {
		return err
	}
	if !usageCountHandler(binary.LittleEndian.Uint16(usageCountBuffer)) {
		return nil
	}
	if _, err := d.conn.Write([]byte{'+'}); err != nil {
		return err
	}

	var blockSize uint32
	if err := binary.Read(d.conn, binary.LittleEndian, &blockSize); err != nil {
		return err
	}

	readBuffer := make([]byte, chunkSize)
	if blockSize < chunkSize {
		readBuffer = make([]byte, blockSize)
	}
	for blockSize > 0 {
		_, err := io.ReadAtLeast(d.conn, readBuffer, len(readBuffer))
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

	if !d.result() {
		return fmt.Errorf("read command is failed on data node")
	}

	if !verifyHandler() {
		return fmt.Errorf("read result is not verified")
	}

	return nil
}

func (d *dataNode) SyncList(readHandler func(fileItem common.SyncFileItem, current uint64, total uint64) error) error {
	if err := d.connect(); err != nil {
		return err
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandSyncList)); err != nil {
		return err
	}

	if !d.result() {
		return fmt.Errorf("data node refused the sync list request")
	}

	var fileItemListLength uint64
	if err := binary.Read(d.conn, binary.LittleEndian, &fileItemListLength); err != nil {
		return err
	}

	for current := uint64(1); current <= fileItemListLength; current++ {
		sha512Hex, err := d.hashAsHex()
		if err != nil {
			return err
		}

		var size int32
		if err := binary.Read(d.conn, binary.LittleEndian, &size); err != nil {
			return err
		}

		if err := readHandler(common.SyncFileItem{
			Sha512Hex: sha512Hex,
			Size:      size,
		}, current, fileItemListLength); err != nil {
			return err
		}
	}

	if !d.result() {
		return fmt.Errorf("sync full command is failed on data node")
	}

	return nil
}

var _ DataNode = &dataNode{}
