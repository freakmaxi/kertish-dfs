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

	"github.com/freakmaxi/kertish-dfs/basics/src/errors"
)

const commandCreate = "CREA"
const commandRead = "READ"
const commandDelete = "DELE"

type DataNode interface {
	Create(data []byte) (bool, string, error)
	CreateShadow(sha512Hex string) error
	Read(sha512Hex string, readHandler func(data []byte) error) error
	Delete(sha512Hex string) error
}

type dataNode struct {
	address *net.TCPAddr
	sha512  hash.Hash

	conn *net.TCPConn
}

func NewDataNode(address string) DataNode {
	addr, _ := net.ResolveTCPAddr("tcp", address)

	return &dataNode{
		address: addr,
		sha512:  sha512.New512_256(),
	}
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
	d.conn.Close()
}

func (d *dataNode) result() bool {
	b := make([]byte, 1)
	_, err := d.conn.Read(b)
	if err != nil {
		return false
	}
	return strings.Compare("+", string(b)) == 0
}

func (d *dataNode) Create(data []byte) (bool, string, error) {
	d.sha512.Reset()

	if err := d.connect(); err != nil {
		return false, "", err
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandCreate)); err != nil {
		return false, "", err
	}

	d.sha512.Write(data)
	sha512Sum := d.sha512.Sum(nil)
	if _, err := d.conn.Write(sha512Sum); err != nil {
		return false, "", err
	}

	if !d.result() {
		return true, hex.EncodeToString(sha512Sum), nil
	}

	blockSize := uint32(len(data))
	if err := binary.Write(d.conn, binary.LittleEndian, &blockSize); err != nil {
		return false, "", err
	}

	if _, err := d.conn.Write(data); err != nil {
		return false, "", err
	}

	if !d.result() {
		return false, "", fmt.Errorf("create command is failed on data cluster")
	}

	return false, hex.EncodeToString(sha512Sum), nil
}

func (d *dataNode) CreateShadow(sha512Hex string) error {
	if err := d.connect(); err != nil {
		return err
	}
	defer d.close()

	if _, err := d.conn.Write([]byte(commandCreate)); err != nil {
		return err
	}

	sha512Sum, _ := hex.DecodeString(sha512Hex)
	if _, err := d.conn.Write(sha512Sum); err != nil {
		return err
	}

	if d.result() {
		return errors.ErrCreate
	}

	return nil
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

	buf := make([]byte, blockSize)

	if _, err = io.ReadAtLeast(d.conn, buf, len(buf)); err != nil {
		return err
	}

	d.sha512.Write(buf)
	if err := readHandler(buf); err != nil {
		return err
	}

	if !d.result() {
		return fmt.Errorf("read command is failed on data cluster")
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
		return fmt.Errorf("delete command is failed on data cluster")
	}

	return nil
}

var _ DataNode = &dataNode{}
