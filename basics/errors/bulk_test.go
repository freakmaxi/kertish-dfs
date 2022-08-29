package errors

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBulkError(t *testing.T) {
	var bulk *BulkError

	funcX := func(err error) {
		if bulk == nil {
			bulk = NewBulkError()
		}
		bulk.Add(err)
	}

	assert.Nil(t, bulk)

	funcX(fmt.Errorf("test1"))
	result := bulk.Error()

	assert.True(t, strings.HasSuffix(result, "test1"))

	funcX(fmt.Errorf("test2"))
	result = bulk.Error()

	assert.True(t, strings.HasSuffix(result, "test2"))
}

func TestBulkError_ContainsType(t *testing.T) {
	bulk := NewBulkError()

	bulk.Add(fmt.Errorf("test1"))
	bulk.Add(NewUploadError("upload Error"))
	bulk.Add(fmt.Errorf("test2"))

	assert.True(t, bulk.ContainsType(&UploadError{}))

	bulk = NewBulkError()

	bulk.Add(fmt.Errorf("test1"))
	bulk.Add(fmt.Errorf("test2"))

	assert.False(t, bulk.ContainsType(&UploadError{}))
}
