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

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/data-node/cache"
	"github.com/freakmaxi/kertish-dfs/data-node/cluster"
	"github.com/freakmaxi/kertish-dfs/data-node/filesystem"
	"github.com/freakmaxi/kertish-dfs/data-node/filesystem/block"
	"github.com/freakmaxi/kertish-dfs/data-node/manager"
	"go.uber.org/zap"
)

const commandBuffer = 4             // 4b
const defaultTransferSpeed = 625000 // bytes/s

type Commander interface {
	Handler(net.Conn)
}

type commander struct {
	fs     filesystem.Manager
	cache  cache.Container
	node   manager.Node
	logger *zap.Logger

	hardwareAddr string
}

func NewCommander(fs filesystem.Manager, cc cache.Container, node manager.Node, logger *zap.Logger, hardwareAddr string) (Commander, error) {
	return &commander{
		fs:           fs,
		cache:        cc,
		node:         node,
		logger:       logger,
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
	defer func() { _ = conn.Close() }()

	buffer := make([]byte, commandBuffer)

	if err := c.readWithTimeout(conn, buffer, len(buffer)); err != nil {
		c.logger.Error(
			"Stream unable to read",
			zap.String("connection", conn.RemoteAddr().String()),
			zap.Error(err),
		)
		return
	}

	if err := c.process(string(buffer), conn); err != nil {
		if err != errors.ErrQuit {
			c.logger.Error(
				"Unable to process command",
				zap.String("command", string(buffer)),
				zap.String("connection", conn.RemoteAddr().String()),
				zap.Error(err),
			)
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
		return c.leav()
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
	case "SSCR":
		return c.sscr()
	case "SSDE":
		return c.ssde(conn)
	case "SSRS":
		return c.ssrs(conn)
	case "WIPE":
		return c.wipe()
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

	var blockUsage uint16 = 1
	var blockSize uint32

	err = c.fs.Block().LockFile(sha512Hex, func(blockFile block.File) error {
		if !blockFile.Temporary() {
			if err := blockFile.IncreaseUsage(); err != nil {
				return err
			}
			blockUsage = blockFile.Usage()

			var err error
			blockSize, err = blockFile.Size()
			if err != nil {
				return err
			}

			return errors.ErrQuit
		}
		if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
			return err
		}

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
	})
	if err != nil && err != errors.ErrQuit {
		return err
	}

	<-c.node.Notify(sha512Hex, blockUsage, blockSize, err == errors.ErrQuit, true)

	return err
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

	return c.fs.Block().File(sha512Hex, func(blockFile block.File) error {
		if blockFile.Temporary() {
			return os.ErrNotExist
		}
		if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
			return err
		}

		size, err := blockFile.Size()
		if err != nil {
			return err
		}

		if err := c.writeBinaryWithTimeout(conn, size); err != nil {
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

	var blockUsage uint16
	var blockSize uint32

	if err := c.fs.Block().LockFile(sha512Hex, func(blockFile block.File) error {
		if blockFile.Temporary() {
			return errors.ErrQuit
		}

		blockUsage = blockFile.Usage()
		blockSize, err = blockFile.Size()
		if err != nil {
			return err
		}

		if err := blockFile.Delete(); err != nil {
			return err
		}
		blockUsage--

		if blockUsage == 0 {
			c.cache.Remove(sha512Hex)
		}

		return nil
	}); err != nil {
		if err == errors.ErrQuit {
			return nil
		}
		return err
	}

	<-c.node.Notify(sha512Hex, blockUsage, blockSize, blockUsage > 0, false)

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

func (c *commander) leav() error {
	c.node.Leave()
	return nil
}

func (c *commander) sycr(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	var sourceAddrLength uint8
	if err := c.readBinaryWithTimeout(conn, &sourceAddrLength); err != nil {
		return err
	}

	sourceAddrBuf := make([]byte, sourceAddrLength)
	if err := c.readWithTimeout(conn, sourceAddrBuf, len(sourceAddrBuf)); err != nil {
		return err
	}
	sourceAddr := string(sourceAddrBuf)

	return c.fs.Sync(func(sync filesystem.Synchronize) error {
		sync.Create(sourceAddr, sha512Hex)
		return nil
	})
}

func (c *commander) syrd(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	var drop bool
	if err := c.readBinaryWithTimeout(conn, &drop); err != nil {
		return err
	}

	var snapshotTimeUint64 uint64
	if err := c.readBinaryWithTimeout(conn, &snapshotTimeUint64); err != nil {
		return err
	}

	blockManager := c.fs.Block()
	if snapshotTimeUint64 > 0 {
		if err := c.fs.Snapshot(func(snapshot filesystem.Snapshot) error {
			snapshotTime, err := snapshot.FromUint(snapshotTimeUint64)
			if err != nil {
				return err
			}

			blockManager, err = snapshot.Block(*snapshotTime)
			return err
		}); err != nil {
			return err
		}
	}

	return blockManager.LockFile(sha512Hex, func(blockFile block.File) error {
		if blockFile.Temporary() {
			return os.ErrNotExist
		}
		if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
			return err
		}

		size, err := blockFile.Size()
		if err != nil {
			return err
		}

		if err := c.writeBinaryWithTimeout(conn, size); err != nil {
			return err
		}

		usage := blockFile.Usage()
		if err := c.writeBinaryWithTimeout(conn, usage); err != nil {
			return err
		}

		if !c.result(conn) {
			return nil
		}

		return blockFile.Read(
			func(data []byte) error {
				return c.writeWithTimeout(conn, data)
			},
			func() error {
				if drop {
					c.cache.Remove(sha512Hex)

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

	return c.fs.Sync(func(sync filesystem.Synchronize) error {
		sync.Delete(sha512Hex)
		return nil
	})
}

func (c *commander) symv(conn net.Conn) error {
	sha512Hex, err := c.hashAsHex(conn)
	if err != nil {
		return err
	}

	var sourceAddrLength uint8
	if err := c.readBinaryWithTimeout(conn, &sourceAddrLength); err != nil {
		return err
	}

	sourceAddrBuf := make([]byte, sourceAddrLength)
	if err := c.readWithTimeout(conn, sourceAddrBuf, len(sourceAddrBuf)); err != nil {
		return err
	}
	sourceAddr := string(sourceAddrBuf)

	return c.fs.Block().LockFile(sha512Hex, func(blockFile block.File) error {
		dn, err := cluster.NewDataNode(sourceAddr)
		if err != nil {
			return err
		}

		return dn.SyncRead(
			nil,
			sha512Hex,
			true,
			func(data []byte) error {
				return blockFile.Write(data)
			},
			func(usage uint16) bool {
				if err := blockFile.ResetUsage(usage); err != nil {
					return false
				}
				return blockFile.Verify()
			})
	})
}

func (c *commander) syls(conn net.Conn) error {
	var snapshotTimeUint uint64
	if err := c.readBinaryWithTimeout(conn, &snapshotTimeUint); err != nil {
		return err
	}

	var snapshotTime *time.Time
	if snapshotTimeUint > 0 {
		_ = c.fs.Snapshot(func(snapshot filesystem.Snapshot) error {
			snapshotTime, _ = snapshot.FromUint(snapshotTimeUint)
			return nil
		})
	}

	var snapshots common.Snapshots
	if err := c.fs.Snapshot(func(snapshot filesystem.Snapshot) error {
		var err error
		snapshots, err = snapshot.Dates()
		return err
	}); err != nil {
		return err
	}

	if err := c.writeWithTimeout(conn, []byte{'+'}); err != nil {
		return err
	}

	snapshotsLength := uint64(len(snapshots))
	if err := c.writeBinaryWithTimeout(conn, snapshotsLength); err != nil {
		return err
	}

	for _, snapshot := range snapshots {
		var snapshotTimeUint uint64
		_ = c.fs.Snapshot(func(s filesystem.Snapshot) error {
			snapshotTimeUint = s.ToUint(snapshot)
			return nil
		})
		if err := c.writeBinaryWithTimeout(conn, snapshotTimeUint); err != nil {
			return err
		}
	}

	if err := c.fs.Sync(func(sync filesystem.Synchronize) error {
		return sync.List(snapshotTime, func(fileItem *common.SyncFileItem) error {
			sha512Sum, err := hex.DecodeString(fileItem.Sha512Hex)
			if err != nil {
				return err
			}

			if err := c.writeWithTimeout(conn, sha512Sum); err != nil {
				return err
			}

			if err := c.writeBinaryWithTimeout(conn, fileItem.Usage); err != nil {
				return err
			}

			if err := c.writeBinaryWithTimeout(conn, fileItem.Size); err != nil {
				return err
			}

			return nil
		})
	}); err != nil {
		return err
	}

	sha512Sum, err := hex.DecodeString(common.NullSha512Hex)
	if err != nil {
		return err
	}

	if err := c.writeWithTimeout(conn, sha512Sum); err != nil {
		return err
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

	if err := c.fs.Sync(func(sync filesystem.Synchronize) error {
		return sync.Full(sourceAddr)
	}); err != nil {
		c.logger.Warn("Sync is failed", zap.String("masterNodeAddress", sourceAddr), zap.Error(err))
		return err
	}

	c.cache.Invalidate()

	return nil
}

func (c *commander) sscr() error {
	err := c.fs.Snapshot(func(snapshot filesystem.Snapshot) error {
		_, err := snapshot.Create(nil)
		return err
	})
	if err != nil {
		c.logger.Error("Snapshot creation is failed", zap.Error(err))
	}
	return err
}

func (c *commander) ssde(conn net.Conn) error {
	var snapshotIndex uint64
	if err := c.readBinaryWithTimeout(conn, &snapshotIndex); err != nil {
		return err
	}

	return c.fs.Snapshot(func(snapshot filesystem.Snapshot) error {
		snapshotDates, err := snapshot.Dates()
		if err != nil {
			return err
		}
		if uint64(len(snapshotDates)) <= snapshotIndex {
			return fmt.Errorf("snapshot index (%d) is out of range", snapshotIndex)
		}
		return snapshot.Delete(snapshotDates[snapshotIndex])
	})
}

func (c *commander) ssrs(conn net.Conn) error {
	var snapshotIndex uint64
	if err := c.readBinaryWithTimeout(conn, &snapshotIndex); err != nil {
		return err
	}

	if err := c.fs.Snapshot(func(snapshot filesystem.Snapshot) error {
		snapshotDates, err := snapshot.Dates()
		if err != nil {
			return err
		}
		if uint64(len(snapshotDates)) <= snapshotIndex {
			return fmt.Errorf("snapshot index (%d) is out of range", snapshotIndex)
		}
		return snapshot.Restore(snapshotDates[snapshotIndex])
	}); err != nil {
		return err
	}

	c.cache.Invalidate()

	return nil
}

func (c *commander) wipe() error {
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

	return c.writeBinaryWithTimeout(conn, c.node.NodeSize())
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
