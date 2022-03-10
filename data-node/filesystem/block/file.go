package block

import (
	"crypto/sha512"
	"encoding/hex"
	"hash"
	"io"
	"os"
	"path"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

const chunkSize uint32 = 1024 * 1024 // 1mb

// File handler for block file operations
type File interface {
	Temporary() bool

	Write(data []byte) error
	Verify() bool
	VerifyForce() bool

	Seek(offset int64) error
	Read(begins uint32, ends uint32, readHandler func(data []byte) error, completedHandler func(inconsistency bool) error) error

	Id() string
	Usage() uint16
	IncreaseUsage() error
	ResetUsage(uint16) error
	Size() (uint32, error)

	Delete() error
	Wipe() error
	Truncate(blockSize uint32) error

	Cancel()
	Close()
}

type file struct {
	inner  *os.File
	header *FileHeader

	sha512   hash.Hash
	tempPath string
	verified bool
	canceled bool

	sha512Hex  string
	targetPath string
	logger     *zap.Logger
}

// NewFile provides the file interface for (new) block file operations
func NewFile(root string, sha512Hex string, logger *zap.Logger) (File, error) {
	file := &file{
		sha512:     sha512.New512_256(),
		sha512Hex:  sha512Hex,
		targetPath: path.Join(root, sha512Hex),
		verified:   true,
		canceled:   false,
		logger:     logger,
	}

	f, err := os.OpenFile(file.targetPath, os.O_RDWR, 0666)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		file.verified = false
		file.tempPath = path.Join(os.TempDir(), uuid.New().String())
		f, err = os.OpenFile(file.tempPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return nil, err
		}
	}
	file.inner = f
	file.header = NewFileHeader(f)

	if err := file.header.Load(); err != nil {
		return nil, err
	}

	return file, nil
}

func (f *file) Temporary() bool {
	return len(f.tempPath) > 0
}

func (f *file) Write(data []byte) error {
	f.verified = false

	if _, err := f.sha512.Write(data); err != nil {
		return err
	}
	_, err := f.inner.Write(data)
	return err
}

func (f *file) Verify() bool {
	if f.verified {
		return f.verified
	}

	result := hex.EncodeToString(f.sha512.Sum(nil))
	f.verified = strings.Compare(result, f.sha512Hex) == 0
	return f.verified
}

func (f *file) VerifyForce() bool {
	f.sha512.Reset()

	if err := f.Read(0, 0,
		func(data []byte) error {
			_, err := f.sha512.Write(data)
			return err
		}, func(_ bool) error {
			result := hex.EncodeToString(f.sha512.Sum(nil))
			f.verified = strings.Compare(result, f.sha512Hex) == 0

			return nil
		}); err != nil {
		return false
	}

	return f.verified
}

func (f *file) Seek(offset int64) error {
	_, err := f.inner.Seek(f.header.Size()+offset, io.SeekStart)
	return err
}

func (f *file) Read(begins uint32, ends uint32, readHandler func(data []byte) error, completedHandler func(inconsistency bool) error) error {
	if begins > 0 {
		if err := f.Seek(int64(begins)); err != nil {
			return err
		}
	}

	total := ^uint32(0) >> 1
	if ends > 0 {
		total = ends - begins
	}

	buffer := make([]byte, chunkSize)
	for total > 0 {
		s, err := f.inner.Read(buffer)
		if err != nil {
			if err == io.EOF {
				return completedHandler(ends > 0 && total != 0)
			}
			return err
		}

		if total < uint32(s) {
			s = int(total)
		}

		if err := readHandler(buffer[0:s]); err != nil {
			return err
		}

		total -= uint32(s)
	}

	return completedHandler(ends > 0 && total != 0)
}

func (f *file) Id() string {
	return f.sha512Hex
}

func (f *file) Usage() uint16 {
	return f.header.Usage()
}

func (f *file) IncreaseUsage() error {
	return f.header.IncreaseUsage()
}

func (f *file) ResetUsage(usage uint16) error {
	return f.header.ResetUsage(usage)
}

func (f *file) Size() (uint32, error) {
	info, err := f.inner.Stat()
	if err != nil {
		return 0, err
	}

	size := uint32(info.Size() - f.header.Size())

	return size, nil
}

func (f *file) Delete() error {
	if err := f.header.DecreaseUsage(); err != nil {
		if err == io.EOF {
			return f.Wipe()
		}
		return err
	}
	return nil
}

func (f *file) Wipe() error {
	return os.Remove(f.targetPath)
}

func (f *file) Truncate(blockSize uint32) error {
	if err := os.Truncate(f.targetPath, int64(blockSize)+f.header.Size()); err != nil {
		return err
	}
	return f.ResetUsage(1)
}

func (f *file) Cancel() {
	f.canceled = true
}

func (f *file) Close() {
	_ = f.inner.Close()

	if !f.verified || f.canceled {
		_ = os.Remove(f.tempPath)
		return
	}

	if !f.Temporary() {
		return
	}

	if err := f.move(f.tempPath, f.targetPath); err != nil {
		f.logger.Error("File creation is failed silently", zap.Error(err))
	}
}

func (f *file) move(source string, target string) error {
	defer func() { _ = os.Remove(source) }()

	sourceFile, err := os.OpenFile(source, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	defer func() { _ = sourceFile.Close() }()

	targetFile, err := os.OpenFile(target, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer func() { _ = targetFile.Close() }()

	if _, err = io.Copy(targetFile, sourceFile); err != nil {
		return err
	}

	return nil
}

var _ File = &file{}
