package cluster

import (
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/errors"
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

func (d *dataNode) Create(data []byte) (exists bool, sha512Hex string, err error) {
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
			exists = true
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

func (d *dataNode) CreateShadow(sha512Hex string) error {
	return d.connect(func(conn *net.TCPConn) error {
		if _, err := conn.Write([]byte(commandCreate)); err != nil {
			return err
		}

		sha512Sum, _ := hex.DecodeString(sha512Hex)
		if _, err := conn.Write(sha512Sum); err != nil {
			return err
		}

		if d.result(conn) {
			return errors.ErrCreate
		}

		return nil
	})
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

		buf := make([]byte, blockSize)

		if _, err = io.ReadAtLeast(conn, buf, len(buf)); err != nil {
			return err
		}

		sha512Hash := sha512.New512_256()
		sha512Hash.Write(buf)

		if err := readHandler(buf); err != nil {
			return err
		}

		if !d.result(conn) {
			return fmt.Errorf("read command is failed on data cluster")
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
			return fmt.Errorf("delete command is failed on data cluster")
		}

		return nil
	})
}

var _ DataNode = &dataNode{}
