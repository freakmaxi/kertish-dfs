package common

import (
	"fmt"
	"strconv"
	"strings"
)

type ReadRange struct {
	Begins int64
	Ends   int64
}

func NewReadRange(value string) (*ReadRange, error) {
	rr := strings.Split(value, "->")

	begins, err := strconv.ParseInt(rr[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("range is not valid")
	}

	if begins < 0 {
		return nil, fmt.Errorf("range is not valid")
	}

	ends, err := strconv.ParseInt(rr[1], 10, 64)
	if err != nil {
		ends = -1
	}

	if ends > -1 && ends < begins {
		return nil, fmt.Errorf("end can not be smaller than begin in range value")
	}

	return &ReadRange{
		Begins: begins,
		Ends:   ends,
	}, nil
}

func (r *ReadRange) HasRange() bool {
	return r.Begins > 0 && r.Ends != -1
}
