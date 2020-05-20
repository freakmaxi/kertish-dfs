package service

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/data-node/cache"
	"github.com/freakmaxi/kertish-dfs/data-node/cluster"
	"github.com/freakmaxi/kertish-dfs/data-node/filesystem"
	"github.com/freakmaxi/kertish-dfs/data-node/manager"
)

const commandBuffer = 4             // 4b
const defaultTransferSpeed = 625000 // bytes/s

type Commander interface {
	Handler(net.Conn)
}

type commander struct {
	fs    filesystem.Manager
	cache cache.Container
	node  manager.Node

	hardwareAddr string
}

func NewCommander(fs filesystem.Manager, cc cache.Container, node manager.Node, hardwareAddr string) (Commander, error) {
	return &commander{
		fs:           fs,
		cache:        cc,
		node:         node,
		hardwareAddr: hardwareAddr,
	}, nil
}

func (c *commander) setDeadline(conn net.Conn, expectedTransferSize int) error {
	seconds := expectedTransferSize / defaultTransferSpeed
	if seconds < 0 {
		seconds = 0
	}
	seconds += 30

	return conn.SetDeadline(time.Now().Add(time.Second * time.Duration(seconds)))
}

func (c *commander) readWithTimeout(conn net.Conn, buffer []byte, size int) error {
	if err := c.setDeadline(conn, size); err != nil {
		return err
	}
	_, err := io.ReadAtLeast(conn, buffer, size)
	return err
}

func (c *commander) readBinaryWithTimeout(conn net.Conn, data interface{}) error {
	if err := c.setDeadline(conn, 0); err != nil {
		return err
	}
	return binary.Read(conn, binary.LittleEndian, data)
}

func (c *commander) writeWithTimeout(conn net.Conn, b []byte) error {
	if err := c.setDeadline(conn, len(b)); err != nil {
		return err
	}
	_, err := conn.Write(b)
	return err
}

func (c *commander) writeBinaryWithTimeout(conn net.Conn, data interface{}) error {
	if err := c.setDeadline(conn, 0); err != nil {
		return err
	}
	return binary.Write(conn, binary.LittleEndian, data)
}

func (c *commander) Handler(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, commandBuffer)

	if err := c.readWithTimeout(conn, buffer, len(buffer)); err != nil {
		fmt.Printf("ERROR: Stream unable to read: Connection: %s, %s\n", conn.RemoteAddr().String(), err.Error())
		return
	}

	if err := c.process(string(buffer), conn); err != nil {
		if err != errors.ErrQuit {
			fmt.Printf("ERROR: Unable to process command (%s): Connection: %s, %s\n", string(buffer), conn.RemoteAddr().String(), err.Error())
		}
		_ = c.writeWithTimeout(conn, []byte("-"))
		return
	}

	_ = c.writeWithTimeout(conn, []byte("+"))
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
	case "SYMV":
		return c.symv(conn)
	case "SYLS":
		return c.syls(conn)
	case "SYFL":
		return c.syfl(conn)
	case "WIPE":
		return c.wipe(conn)
	case "SIZE":
		return c.size(conn)
	case "USED":
		return c.used(conn)
	case "PING":
		return nil
	default:
		return fmt.Errorf("not a meaningful command")
	}
}

func (c *commander) hashAsHex(conn net.Conn) (string, error) {
	h := make([]byte, 32)
	err := c.readWithTimeout(conn, h, len(h))
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h), nil
}

func (c *commander) result(conn net.Conn) bool {
	b := make([]byte, 1)
	if err := c.readWithTimeout(conn, b, len(b)); err != nil {
		return false
	}

	return strings.Compare("+", string(b)) == 0
}

func (c *commander) crea(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	if err := c.fs.LockFile(sha512Hex, func(blockFile filesystem.BlockFile) error {
		if !blockFile.Temporary() {
			if err := blockFile.Mark(); err != nil {
				return err
			}
			return errors.ErrQuit
		}
		if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
			return err
		}

		var blockSize uint32
		if err := c.readBinaryWithTimeout(conn, &blockSize); err != nil {
			return err
		}

		chunkBuffer := make([]byte, blockSize)
		if err := c.readWithTimeout(conn, chunkBuffer, len(chunkBuffer)); err != nil {
			return err
		}

		if err := blockFile.Write(chunkBuffer); err != nil {
			return err
		}

		if !blockFile.Verify() {
			return fmt.Errorf("file is not verified")
		}

		// Add to the cache in go routine
		go c.cache.Upsert(sha512Hex, chunkBuffer)

		return nil
	}); err != nil && err != errors.ErrQuit {
		return err
	}

	c.node.Create(sha512Hex)
	return nil
}

func (c *commander) read(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	// Check cache first
	if content := c.cache.Query(sha512Hex); content != nil {
		if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
			return err
		}

		sizeBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(sizeBuffer, uint32(len(content)))

		if err := c.writeWithTimeout(conn, sizeBuffer); err != nil {
			return err
		}

		if err := c.writeWithTimeout(conn, content); err != nil {
			return err
		}

		return nil
	}

	return c.fs.File(sha512Hex, func(blockFile filesystem.BlockFile) error {
		if blockFile.Temporary() {
			return os.ErrNotExist
		}
		if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
			return err
		}

		_, sizeBuffer, err := blockFile.Size()
		if err != nil {
			return err
		}

		if err := c.writeWithTimeout(conn, sizeBuffer); err != nil {
			return err
		}

		cacheData := make([]byte, 0)
		return blockFile.Read(
			func(data []byte) error {
				// Compile For Cache
				cacheData = append(cacheData, data...)

				return c.writeWithTimeout(conn, data)
			},
			func() error {
				// Add/Update Cache
				c.cache.Upsert(sha512Hex, cacheData)

				return nil
			})
	})
}

func (c *commander) dele(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	deleteShadow := false
	deleteSize := uint32(0)

	if err := c.fs.LockFile(sha512Hex, func(blockFile filesystem.BlockFile) error {
		if blockFile.Temporary() {
			return errors.ErrQuit
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

		// Remove from cache
		c.cache.Remove(sha512Hex)

		deleteShadow = usageCount > 1
		deleteSize = size

		return nil
	}); err != nil {
		if err == errors.ErrQuit {
			return nil
		}
		return err
	}

	c.node.Delete(sha512Hex, deleteShadow, deleteSize)
	return nil
}

func (c *commander) hwid(conn net.Conn) error {
	if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
		return err
	}

	hardwareIdLength := byte(len(c.hardwareAddr))
	if err := c.writeBinaryWithTimeout(conn, hardwareIdLength); err != nil {
		return err
	}

	if err := c.writeWithTimeout(conn, []byte(c.hardwareAddr)); err != nil {
		return err
	}

	return nil
}

func (c *commander) join(conn net.Conn) error {
	var clusterIdLength uint8
	if err := c.readBinaryWithTimeout(conn, &clusterIdLength); err != nil {
		return err
	}

	bC := make([]byte, clusterIdLength)
	if err := c.readWithTimeout(conn, bC, len(bC)); err != nil {
		return err
	}

	var nodeIdLength uint8
	if err := c.readBinaryWithTimeout(conn, &nodeIdLength); err != nil {
		return err
	}

	bN := make([]byte, nodeIdLength)
	if err := c.readWithTimeout(conn, bN, len(bN)); err != nil {
		return err
	}

	var masterAddrLength uint8
	if err := c.readBinaryWithTimeout(conn, &masterAddrLength); err != nil {
		return err
	}

	bM := make([]byte, masterAddrLength)
	if err := c.readWithTimeout(conn, bM, len(bM)); err != nil {
		return err
	}

	c.node.Join(string(bC), string(bN), string(bM))

	return nil
}

func (c *commander) mode(conn net.Conn) error {
	var master bool
	if err := c.readBinaryWithTimeout(conn, &master); err != nil {
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
	if err := c.readBinaryWithTimeout(conn, &sourceAddrLength); err != nil {
		return err
	}

	sourceAddrBuf := make([]byte, sourceAddrLength)
	if err := c.readWithTimeout(conn, sourceAddrBuf, len(sourceAddrBuf)); err != nil {
		return err
	}
	sourceAddr := string(sourceAddrBuf)

	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	return c.fs.LockFile(sha512Hex, func(blockFile filesystem.BlockFile) error {
		usageCountBackup := uint16(1)

		dn, err := cluster.NewDataNode(sourceAddr)
		if err != nil {
			return err
		}

		return dn.SyncRead(
			sha512Hex,
			false,
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
	var drop bool
	if err := c.readBinaryWithTimeout(conn, &drop); err != nil {
		return err
	}

	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	return c.fs.File(sha512Hex, func(blockFile filesystem.BlockFile) error {
		if blockFile.Temporary() {
			return os.ErrNotExist
		}
		if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
			return err
		}

		_, usageCountBuffer, err := blockFile.Usage()
		if err != nil {
			return err
		}

		if err := c.writeWithTimeout(conn, usageCountBuffer); err != nil {
			return err
		}

		if !c.result(conn) {
			return nil
		}

		_, sizeBuffer, err := blockFile.Size()
		if err != nil {
			return err
		}

		if err := c.writeWithTimeout(conn, sizeBuffer); err != nil {
			return err
		}

		return blockFile.Read(
			func(data []byte) error {
				return c.writeWithTimeout(conn, data)
			},
			func() error {
				if drop {
					return blockFile.Wipe()
				}
				return nil
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

func (c *commander) symv(conn net.Conn) error {
	var sourceAddrLength uint8
	if err := c.readBinaryWithTimeout(conn, &sourceAddrLength); err != nil {
		return err
	}

	sourceAddrBuf := make([]byte, sourceAddrLength)
	if err := c.readWithTimeout(conn, sourceAddrBuf, len(sourceAddrBuf)); err != nil {
		return err
	}
	sourceAddr := string(sourceAddrBuf)

	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	return c.fs.LockFile(sha512Hex, func(blockFile filesystem.BlockFile) error {
		usageCountBackup := uint16(1)

		dn, err := cluster.NewDataNode(sourceAddr)
		if err != nil {
			return err
		}

		return dn.SyncRead(
			sha512Hex,
			true,
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

func (c *commander) syls(conn net.Conn) error {
	sha512HexList, err := c.fs.List()
	if err != nil {
		return err
	}

	if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
		return err
	}

	sha512HexListLength := uint64(len(sha512HexList))
	if err := c.writeBinaryWithTimeout(conn, sha512HexListLength); err != nil {
		return err
	}

	for _, file := range sha512HexList {
		sha512Sum, err := hex.DecodeString(file)
		if err != nil {
			return err
		}

		if err := c.writeWithTimeout(conn, sha512Sum); err != nil {
			return err
		}
	}

	return nil
}

func (c *commander) syfl(conn net.Conn) error {
	var sourceAddrLength uint8
	if err := c.readBinaryWithTimeout(conn, &sourceAddrLength); err != nil {
		return err
	}

	sourceAddrBuf := make([]byte, sourceAddrLength)
	if err := c.readWithTimeout(conn, sourceAddrBuf, len(sourceAddrBuf)); err != nil {
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
	if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
		return err
	}

	return c.writeBinaryWithTimeout(conn, c.fs.NodeSize())
}

func (c *commander) used(conn net.Conn) error {
	used, err := c.fs.Used()
	if err != nil {
		return err
	}

	if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
		return err
	}

	return c.writeBinaryWithTimeout(conn, used)
}

var _ Commander = &commander{}
