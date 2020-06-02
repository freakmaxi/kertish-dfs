package block

import (
	"encoding/binary"
	"io"
	"os"
)

const headerSize int64 = 2

type FileHeader struct {
	inner *os.File

	usage uint16 // 2 bytes
}

func NewFileHeader(file *os.File) *FileHeader {
	return &FileHeader{
		inner: file,
		usage: 1,
	}
}

func (h *FileHeader) Size() int64 {
	return headerSize
}

func (h *FileHeader) Load() error {
	if _, err := h.inner.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var usage uint16
	if err := binary.Read(h.inner, binary.LittleEndian, &usage); err != nil {
		if err == io.EOF {
			return h.save()
		}
		return err
	}
	h.usage = usage

	return nil
}

func (h *FileHeader) Usage() uint16 {
	return h.usage
}

func (h *FileHeader) IncreaseUsage() error {
	h.usage++
	return h.save()
}

func (h *FileHeader) DecreaseUsage() error {
	h.usage--
	if h.usage == 0 {
		return io.EOF
	}

	return h.save()
}

func (h *FileHeader) ResetUsage(usage uint16) error {
	if usage < 1 {
		usage = 1
	}

	h.usage = usage
	return h.save()
}

func (h *FileHeader) save() error {
	if _, err := h.inner.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := binary.Write(h.inner, binary.LittleEndian, h.usage); err != nil {
		return err
	}

	return nil
}
