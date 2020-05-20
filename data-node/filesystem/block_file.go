package filesystem

import (
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"strings"

	"github.com/google/uuid"
)

const headerSize int64 = 2
const chunkSize uint32 = 1024 * 1024 // 1mb

type BlockFile interface {
	Temporary() bool

	Write(data []byte) error
	Verify() bool

	Read(readHandler func(data []byte) error, completedHandler func() error) error
	Usage() (uint16, []byte, error)
	Size() (uint32, []byte, error)

	Delete() error
	Wipe() error

	Mark() error
	ResetUsage(count uint16) error

	Cancel()
	Close()
}

type blockFile struct {
	inner *os.File

	sha512   hash.Hash
	tempPath string
	prepared bool
	verified bool
	canceled bool

	sha512Hex  string
	targetPath string
}

func NewBlockFile(root string, sha512Hex string) (BlockFile, error) {
	blockFile := &blockFile{
		sha512:     sha512.New512_256(),
		sha512Hex:  sha512Hex,
		targetPath: path.Join(root, sha512Hex),
		prepared:   true,
		verified:   true,
		canceled:   false,
	}

	file, err := os.OpenFile(blockFile.targetPath, os.O_RDWR, 0666)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		blockFile.prepared = false
		blockFile.verified = false
		blockFile.tempPath = path.Join(os.TempDir(), uuid.New().String())
		file, err = os.OpenFile(blockFile.tempPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return nil, err
		}
	}
	blockFile.inner = file

	return blockFile, nil
}

func (b *blockFile) prepare() error {
	if b.prepared {
		return nil
	}

	if _, err := b.inner.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := binary.Write(b.inner, binary.LittleEndian, uint16(1)); err != nil {
		return err
	}

	b.prepared = true
	return nil
}

func (b *blockFile) Temporary() bool {
	return len(b.tempPath) > 0
}

func (b *blockFile) Write(data []byte) error {
	if err := b.prepare(); err != nil {
		return err
	}

	if _, err := b.sha512.Write(data); err != nil {
		return err
	}
	_, err := b.inner.Write(data)
	return err
}

func (b *blockFile) Verify() bool {
	if b.verified {
		return b.verified
	}

	result := hex.EncodeToString(b.sha512.Sum(nil))
	b.verified = strings.Compare(result, b.sha512Hex) == 0
	return b.verified
}

func (b *blockFile) Read(readHandler func(data []byte) error, completedHandler func() error) error {
	if err := b.prepare(); err != nil {
		return err
	}

	if _, err := b.inner.Seek(headerSize, io.SeekStart); err != nil {
		return err
	}

	buffer := make([]byte, chunkSize)
	for {
		s, err := b.inner.Read(buffer)
		if err != nil {
			if err == io.EOF {
				return completedHandler()
			}
			return err
		}

		if err := readHandler(buffer[0:s]); err != nil {
			return err
		}
	}
}

func (b *blockFile) Usage() (uint16, []byte, error) {
	if err := b.prepare(); err != nil {
		return 0, nil, err
	}

	if _, err := b.inner.Seek(0, io.SeekStart); err != nil {
		return 0, nil, err
	}

	usageCountBuffer := make([]byte, headerSize)
	if _, err := io.ReadAtLeast(b.inner, usageCountBuffer, len(usageCountBuffer)); err != nil {
		return 0, nil, err
	}

	return binary.LittleEndian.Uint16(usageCountBuffer), usageCountBuffer, nil
}

func (b *blockFile) Size() (uint32, []byte, error) {
	info, err := b.inner.Stat()
	if err != nil {
		return 0, nil, err
	}

	size := uint32(info.Size() - headerSize) // two bytes for usageCount
	sizeBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuffer, size)

	return size, sizeBuffer, nil
}

func (b *blockFile) Delete() error {
	usageCount, _, err := b.Usage()
	if err != nil {
		return err
	}
	usageCount--

	if usageCount == 0 {
		return b.Wipe()
	}

	if _, err := b.inner.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return binary.Write(b.inner, binary.LittleEndian, usageCount)
}

func (b *blockFile) Wipe() error {
	return os.Remove(b.targetPath)
}

func (b *blockFile) Mark() error {
	usageCount, _, err := b.Usage()
	if err != nil {
		return err
	}
	usageCount++

	if _, err := b.inner.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return binary.Write(b.inner, binary.LittleEndian, usageCount)
}

func (b *blockFile) ResetUsage(count uint16) error {
	if err := b.prepare(); err != nil {
		return err
	}

	if _, err := b.inner.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return binary.Write(b.inner, binary.LittleEndian, count)
}

func (b *blockFile) Cancel() {
	b.canceled = true
}

func (b *blockFile) Close() {
	b.inner.Close()

	if !b.verified || b.canceled {
		os.Remove(b.tempPath)
		return
	}

	if !b.Temporary() {
		return
	}

	if err := b.move(b.tempPath, b.targetPath); err != nil {
		fmt.Printf("ERROR: File creation is failed silently: %s\n", err.Error())
	}
}

func (b *blockFile) move(source string, target string) error {
	defer os.Remove(source)

	sourceFile, err := os.OpenFile(source, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	targetFile, err := os.OpenFile(target, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer targetFile.Close()

	if _, err = io.Copy(targetFile, sourceFile); err != nil {
		return err
	}

	return nil
}

var _ BlockFile = &blockFile{}
