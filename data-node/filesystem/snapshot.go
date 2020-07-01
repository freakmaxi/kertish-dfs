package filesystem

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	dnc "github.com/freakmaxi/kertish-dfs/data-node/common"
	"github.com/freakmaxi/kertish-dfs/data-node/filesystem/block"
	"go.uber.org/zap"
)

const snapshotPrefix = "snapshot."
const snapshotHeaderBackupFile = "headers.backup"

type Snapshot interface {
	Create(targetSnapshot *time.Time) (*time.Time, error)
	Delete(targetSnapshot time.Time) error
	Restore(sourceSnapshot time.Time) error

	Block(snapshot time.Time) (block.Manager, error)

	ReadHeaderBackup(snapshot time.Time) (HeaderMap, error)
	ReplaceHeaderBackup(snapshot time.Time, headerMap HeaderMap) error

	Latest() (*time.Time, error)
	Dates() (common.Snapshots, error)

	PathName(snapshot time.Time) string
	FromUint(snapshotUint uint64) (*time.Time, error)
	ToUint(snapshot time.Time) uint64
}

type HeaderMap map[string]uint16

type snapshot struct {
	rootPath string
	logger   *zap.Logger

	blocksMutex sync.Mutex
	blocks      map[time.Time]block.Manager
}

func NewSnapshot(rootPath string, logger *zap.Logger) Snapshot {
	return &snapshot{
		rootPath:    rootPath,
		logger:      logger,
		blocksMutex: sync.Mutex{},
		blocks:      make(map[time.Time]block.Manager),
	}
}

func (s *snapshot) ReadHeaderBackup(snapshot time.Time) (HeaderMap, error) {
	headerMap := make(HeaderMap)

	snapshotPathName := s.PathName(snapshot)
	snapshotPath := path.Join(s.rootPath, snapshotPathName)

	headerBackupFilePath := path.Join(snapshotPath, snapshotHeaderBackupFile)
	headerFile, err := os.OpenFile(headerBackupFilePath, os.O_RDONLY, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			return headerMap, nil
		}
		return nil, err
	}
	defer func() { _ = headerFile.Close() }()

	sha512HexBytes := make([]byte, 32)
	var usage uint16

	for {
		if _, err := io.ReadAtLeast(headerFile, sha512HexBytes, len(sha512HexBytes)); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if err := binary.Read(headerFile, binary.LittleEndian, &usage); err != nil {
			return nil, err
		}

		sha512Hex := hex.EncodeToString(sha512HexBytes)
		headerMap[sha512Hex] = usage
	}

	return headerMap, nil
}

func (s *snapshot) ReplaceHeaderBackup(snapshot time.Time, headerMap HeaderMap) error {
	snapshotPathName := s.PathName(snapshot)
	snapshotPath := path.Join(s.rootPath, snapshotPathName)

	headerBackupFilePath := path.Join(snapshotPath, snapshotHeaderBackupFile)
	headerFile, err := os.OpenFile(headerBackupFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer func() { _ = headerFile.Close() }()

	for sha512Hex, usage := range headerMap {
		sha512HexBytes, err := hex.DecodeString(sha512Hex)
		if err != nil {
			return err
		}
		if _, err := headerFile.Write(sha512HexBytes); err != nil {
			return err
		}

		if err := binary.Write(headerFile, binary.LittleEndian, usage); err != nil {
			return err
		}
	}

	return nil
}

func (s *snapshot) Create(targetSnapshot *time.Time) (snapshotTime *time.Time, snapshotErr error) {
	nextSnapshot := time.Now().UTC()
	if targetSnapshot != nil {
		nextSnapshot = *targetSnapshot
	}
	nextSnapshotPath := path.Join(s.rootPath, s.PathName(nextSnapshot))

	_, err := os.Stat(nextSnapshotPath)
	if err == nil {
		return nil, os.ErrExist
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(nextSnapshotPath, 0777); err != nil {
		return nil, err
	}
	defer func() {
		if snapshotErr == nil {
			return
		}

		if err := s.Delete(nextSnapshot); err != nil {
			s.logger.Error("Unable to delete snapshot path", zap.Error(err))
		}
	}()

	s.logger.Info(
		fmt.Sprintf("Creating snapshot as %d", s.ToUint(nextSnapshot)),
		zap.Time("snapshot", nextSnapshot),
	)

	headerBackupFilePath := path.Join(nextSnapshotPath, snapshotHeaderBackupFile)
	headerFile, err := os.OpenFile(headerBackupFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	defer func() { _ = headerFile.Close() }()

	s.logger.Info("Start traversing for snapshot creation")

	if err := dnc.Traverse(s.rootPath, func(info os.FileInfo) error {
		sha512Hex := info.Name()

		blockFile, err := block.NewFile(s.rootPath, sha512Hex, s.logger)
		if err != nil {
			return err
		}
		defer blockFile.Close()

		if blockFile.Temporary() {
			return nil
		}

		sha512HexBytes, err := hex.DecodeString(sha512Hex)
		if err != nil {
			return err
		}
		if _, err := headerFile.Write(sha512HexBytes); err != nil {
			return err
		}

		if err := binary.Write(headerFile, binary.LittleEndian, blockFile.Usage()); err != nil {
			return err
		}

		return os.Link(path.Join(s.rootPath, sha512Hex), path.Join(nextSnapshotPath, sha512Hex))
	}); err != nil {
		return nil, err
	}

	s.logger.Info("Snapshot creation is completed")

	return &nextSnapshot, nil
}

func (s *snapshot) Delete(targetSnapshot time.Time) error {
	targetSnapshotPathName := s.PathName(targetSnapshot)
	targetSnapshotPath := path.Join(s.rootPath, targetSnapshotPathName)

	return os.RemoveAll(targetSnapshotPath)
}

func (s *snapshot) Restore(sourceSnapshot time.Time) error {
	s.logger.Info(
		fmt.Sprintf("Restoring snapshot (%d)", s.ToUint(sourceSnapshot)),
		zap.Time("snapshot", sourceSnapshot),
	)

	sourceSnapshotPathName := s.PathName(sourceSnapshot)
	sourceSnapshotPath := path.Join(s.rootPath, sourceSnapshotPathName)

	_, err := os.Stat(sourceSnapshotPath)
	if err != nil {
		return err
	}

	targetBlock, err := block.NewManager(s.rootPath, s.logger)
	if err != nil {
		return err
	}

	sourceHeaderMap, err := s.ReadHeaderBackup(sourceSnapshot)
	if err != nil {
		return err
	}

	sourceBlock, err := block.NewManager(sourceSnapshotPath, s.logger)
	if err != nil {
		return err
	}

	if err := targetBlock.Wipe(); err != nil {
		return err
	}

	if err := sourceBlock.Traverse(func(sourceFile block.File) error {
		return targetBlock.LockFile(sourceFile.Id(), func(targetFile block.File) error {
			if !targetFile.Temporary() {
				usage, has := sourceHeaderMap[sourceFile.Id()]
				if !has {
					usage = sourceFile.Usage()
				}

				if err := targetFile.ResetUsage(usage); err != nil {
					return err
				}

				if targetFile.VerifyForce() {
					return nil
				}

				// Fallback
				if err := targetFile.Seek(0); err != nil {
					return err
				}
			}

			return sourceFile.Read(
				func(data []byte) error {
					return targetFile.Write(data)
				},
				func() error {
					usage, has := sourceHeaderMap[sourceFile.Id()]
					if !has {
						usage = sourceFile.Usage()
					}

					if err := targetFile.ResetUsage(usage); err != nil {
						return err
					}

					if !targetFile.Verify() {
						return fmt.Errorf("file is not verified")
					}

					return nil
				},
			)
		})
	}); err != nil {
		s.logger.Error(
			fmt.Sprintf("Restoring snapshot (%d) is failed", s.ToUint(sourceSnapshot)),
			zap.Error(err),
		)
		return err
	}

	s.logger.Info(
		fmt.Sprintf("Restoring snapshot (%d) is completed", s.ToUint(sourceSnapshot)),
		zap.Time("snapshot", sourceSnapshot),
	)

	return nil
}

func (s *snapshot) Block(snapshot time.Time) (block.Manager, error) {
	s.blocksMutex.Lock()
	defer s.blocksMutex.Unlock()

	b, has := s.blocks[snapshot]
	if !has {
		snapshotPathName := s.PathName(snapshot)
		snapshotPath := path.Join(s.rootPath, snapshotPathName)

		var err error
		b, err = block.NewManager(snapshotPath, s.logger)
		if err != nil {
			return nil, err
		}

		s.blocks[snapshot] = b
	}

	return b, nil
}

func (s *snapshot) Latest() (*time.Time, error) {
	snapshots, err := s.Dates()
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, nil
	}

	return &snapshots[len(snapshots)-1], nil
}

func (s *snapshot) Dates() (common.Snapshots, error) {
	infos, err := ioutil.ReadDir(s.rootPath)
	if err != nil {
		return nil, err
	}

	snapshots := make(common.Snapshots, 0)
	for _, info := range infos {
		name := info.Name()
		if !info.IsDir() || !strings.HasPrefix(name, snapshotPrefix) {
			continue
		}

		snapshot := name[len(snapshotPrefix):]
		snapshotTime, err := time.Parse(common.MachineTimeFormatWithSeconds, snapshot)
		if err == nil {
			snapshots = append(snapshots, snapshotTime)
		}
	}
	sort.Sort(snapshots)

	return snapshots, nil
}

func (s *snapshot) PathName(snapshot time.Time) string {
	return fmt.Sprintf("%s%s", snapshotPrefix, snapshot.Format(common.MachineTimeFormatWithSeconds))
}

func (s *snapshot) FromUint(snapshotUint uint64) (*time.Time, error) {
	snapshotTime, err := time.Parse(common.MachineTimeFormatWithSeconds, strconv.FormatUint(snapshotUint, 10))
	if err != nil {
		return nil, err
	}
	return &snapshotTime, nil
}

func (s *snapshot) ToUint(snapshot time.Time) uint64 {
	snapshotUint, _ := strconv.ParseUint(snapshot.Format(common.MachineTimeFormatWithSeconds), 10, 64)
	return snapshotUint
}
