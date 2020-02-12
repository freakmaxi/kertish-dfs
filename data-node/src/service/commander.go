package service

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/data-node/src/cluster"
	"github.com/freakmaxi/kertish-dfs/data-node/src/errors"
	"github.com/freakmaxi/kertish-dfs/data-node/src/filesystem"
	"github.com/freakmaxi/kertish-dfs/data-node/src/manager"
)

const commandBuffer = 4 // 4b

type Commander interface {
	Handler(net.Conn)
}

type commander struct {
	fs   filesystem.Manager
	node manager.Node

	hardwareAddr string
}

func NewCommander(fs filesystem.Manager, node manager.Node, hardwareAddr string) (Commander, error) {
	return &commander{
		fs:           fs,
		node:         node,
		hardwareAddr: hardwareAddr,
	}, nil
}

func (c *commander) Handler(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, commandBuffer)

	_, err := io.ReadAtLeast(conn, buffer, len(buffer))
	if err != nil {
		fmt.Printf("ERROR: Stream unable to read: Connection: %s, %s\n", conn.RemoteAddr().String(), err.Error())
		return
	}

	if err := c.process(string(buffer), conn); err != nil {
		if err != errors.ErrQuit {
			fmt.Printf("ERROR: Unable to process command (%s): Connection: %s, %s\n", string(buffer), conn.RemoteAddr().String(), err.Error())
		}
		conn.Write([]byte("-"))
		return
	}

	conn.Write([]byte("+"))
}

func (c *commander) process(command string, conn net.Conn) error {
	switch command {
	case "CREA":
		return c.crea(conn)
	case "READ":
		return c.read(conn)
	case "DELE":
		return c.dele(conn)
	case "HWID":
		return c.hwid(conn)
	case "JOIN":
		return c.join(conn)
	case "MODE":
		return c.mode(conn)
	case "LEAV":
		return c.leav(conn)
	case "SYCR":
		return c.sycr(conn)
	case "SYRD":
		return c.syrd(conn)
	case "SYDE":
		return c.syde(conn)
	case "SYLS":
		return c.syls(conn)
	case "SYFL":
		return c.syfl(conn)
	case "WIPE":
		return c.wipe(conn)
	case "SIZE":
		return c.size(conn)
	case "PING":
		return nil
	default:
		return fmt.Errorf("not a meaningful command")
	}
}

func (c *commander) hashAsHex(conn net.Conn) (string, error) {
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

func (c *commander) result(conn net.Conn) bool {
	b := make([]byte, 1)
	_, err := conn.Read(b)
	if err != nil {
		return false
	}

	return strings.Compare("+", string(b)) == 0
}

func (c *commander) crea(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	return c.fs.LockFile(sha512Hex, func(blockFile filesystem.BlockFile) error {
		if !blockFile.Temporary() {
			if err := blockFile.Mark(); err != nil {
				return err
			}

			if err := c.node.Create(sha512Hex); err != nil {
				return err
			}

			return errors.ErrQuit
		}
		if _, err := conn.Write([]byte{'+'}); err != nil {
			return err
		}

		var blockSize uint32
		if err := binary.Read(conn, binary.LittleEndian, &blockSize); err != nil {
			return err
		}

		chunkBuffer := make([]byte, blockSize)
		if _, err := io.ReadAtLeast(conn, chunkBuffer, len(chunkBuffer)); err != nil {
			return err
		}

		if err := blockFile.Write(chunkBuffer); err != nil {
			return err
		}

		if !blockFile.Verify() {
			return fmt.Errorf("file is not verified")
		}

		if err := c.node.Create(sha512Hex); err != nil {
			blockFile.Cancel()
			return err
		}

		return nil
	})
}

func (c *commander) read(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	return c.fs.File(sha512Hex, func(blockFile filesystem.BlockFile) error {
		if blockFile.Temporary() {
			return os.ErrNotExist
		}
		if _, err := conn.Write([]byte{'+'}); err != nil {
			return err
		}

		_, sizeBuffer, err := blockFile.Size()
		if err != nil {
			return err
		}

		if _, err := conn.Write(sizeBuffer); err != nil {
			return err
		}

		return blockFile.Read(func(data []byte) error {
			_, err := conn.Write(data)
			return err
		})
	})
}

func (c *commander) dele(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	return c.fs.LockFile(sha512Hex, func(blockFile filesystem.BlockFile) error {
		if blockFile.Temporary() {
			return nil
		}

		usageCount, _, err := blockFile.Usage()
		if err != nil {
			return err
		}

		size := uint32(0)
		if usageCount == 1 {
			size, _, err = blockFile.Size()
			if err != nil {
				return err
			}
		}

		if err := blockFile.Delete(); err != nil {
			return err
		}

		// No need to track errors on this call
		c.node.Delete(sha512Hex, usageCount > 1, size)

		return nil
	})
}

func (c *commander) hwid(conn net.Conn) error {
	if _, err := conn.Write([]byte{'+'}); err != nil {
		return err
	}

	hardwareIdLength := byte(len(c.hardwareAddr))
	if err := binary.Write(conn, binary.LittleEndian, hardwareIdLength); err != nil {
		return err
	}

	if _, err := conn.Write([]byte(c.hardwareAddr)); err != nil {
		return err
	}

	return nil
}

func (c *commander) join(conn net.Conn) error {
	var clusterIdLength uint8
	if err := binary.Read(conn, binary.LittleEndian, &clusterIdLength); err != nil {
		return err
	}

	bC := make([]byte, clusterIdLength)
	_, err := io.ReadAtLeast(conn, bC, len(bC))
	if err != nil {
		return err
	}

	var nodeIdLength uint8
	if err := binary.Read(conn, binary.LittleEndian, &nodeIdLength); err != nil {
		return err
	}

	bN := make([]byte, nodeIdLength)
	_, err = io.ReadAtLeast(conn, bN, len(bN))
	if err != nil {
		return err
	}

	var masterAddrLength uint8
	if err := binary.Read(conn, binary.LittleEndian, &masterAddrLength); err != nil {
		return err
	}

	bM := make([]byte, masterAddrLength)
	_, err = io.ReadAtLeast(conn, bM, len(bM))
	if err != nil {
		return err
	}

	c.node.Join(string(bC), string(bN), string(bM))

	return nil
}

func (c *commander) mode(conn net.Conn) error {
	var master bool
	if err := binary.Read(conn, binary.LittleEndian, &master); err != nil {
		return err
	}

	c.node.Mode(master)

	return nil
}

func (c *commander) leav(conn net.Conn) error {
	c.node.Leave()
	return nil
}

func (c *commander) sycr(conn net.Conn) error {
	var sourceAddrLength uint8
	if err := binary.Read(conn, binary.LittleEndian, &sourceAddrLength); err != nil {
		return err
	}

	sourceAddrBuf := make([]byte, sourceAddrLength)
	if _, err := io.ReadAtLeast(conn, sourceAddrBuf, len(sourceAddrBuf)); err != nil {
		return err
	}
	sourceAddr := string(sourceAddrBuf)

	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	return c.fs.LockFile(sha512Hex, func(blockFile filesystem.BlockFile) error {
		usageCountBackup := uint16(1)

		return cluster.NewDataNode(sourceAddr).SyncRead(
			sha512Hex,
			func(usageCount uint16) bool {
				usageCountBackup = usageCount

				if blockFile.Temporary() {
					return true
				}

				return blockFile.ResetUsage(usageCount) != nil
			},
			func(data []byte) error {
				return blockFile.Write(data)
			},
			func() bool {
				if usageCountBackup == 1 {
					return blockFile.Verify()
				}

				if err := blockFile.ResetUsage(usageCountBackup); err != nil {
					return false
				}
				return blockFile.Verify()
			})
	})
}

func (c *commander) syrd(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	return c.fs.File(sha512Hex, func(blockFile filesystem.BlockFile) error {
		if blockFile.Temporary() {
			return os.ErrNotExist
		}
		if _, err := conn.Write([]byte{'+'}); err != nil {
			return err
		}

		_, usageCountBuffer, err := blockFile.Usage()
		if err != nil {
			return err
		}

		if _, err := conn.Write(usageCountBuffer); err != nil {
			return err
		}

		if !c.result(conn) {
			return nil
		}

		_, sizeBuffer, err := blockFile.Size()
		if err != nil {
			return err
		}

		if _, err := conn.Write(sizeBuffer); err != nil {
			return err
		}

		return blockFile.Read(func(data []byte) error {
			_, err := conn.Write(data)
			return err
		})
	})
}

func (c *commander) syde(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	return c.fs.LockFile(sha512Hex, func(blockFile filesystem.BlockFile) error {
		if blockFile.Temporary() {
			return nil
		}

		if err := blockFile.Delete(); err != nil {
			return err
		}

		return nil
	})
}

func (c *commander) syls(conn net.Conn) error {
	sha512HexList, err := c.fs.List()
	if err != nil {
		return err
	}

	if _, err := conn.Write([]byte{'+'}); err != nil {
		return err
	}

	sha512HexListLength := uint64(len(sha512HexList))
	if err := binary.Write(conn, binary.LittleEndian, sha512HexListLength); err != nil {
		return err
	}

	for _, file := range sha512HexList {
		sha512Sum, err := hex.DecodeString(file)
		if err != nil {
			return err
		}

		if _, err := conn.Write(sha512Sum); err != nil {
			return err
		}
	}

	return nil
}

func (c *commander) syfl(conn net.Conn) error {
	var sourceAddrLength uint8
	if err := binary.Read(conn, binary.LittleEndian, &sourceAddrLength); err != nil {
		return err
	}

	sourceAddrBuf := make([]byte, sourceAddrLength)
	if _, err := io.ReadAtLeast(conn, sourceAddrBuf, len(sourceAddrBuf)); err != nil {
		return err
	}
	sourceAddr := string(sourceAddrBuf)

	if err := c.fs.Sync(sourceAddr); err != nil {
		return err
	}

	return nil
}

func (c *commander) wipe(conn net.Conn) error {
	if err := c.fs.Wipe(); err != nil {
		return err
	}
	c.node.Leave()

	return nil
}

func (c *commander) size(conn net.Conn) error {
	if _, err := conn.Write([]byte{'+'}); err != nil {
		return err
	}

	return binary.Write(conn, binary.LittleEndian, c.fs.NodeSize())
}

var _ Commander = &commander{}
