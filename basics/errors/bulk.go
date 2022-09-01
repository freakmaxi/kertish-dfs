package errors

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

// BulkError struct is to keep multiple error that can be occurred in execution
type BulkError struct {
	innerErrors []*errorContainer

	mutex *sync.Mutex
}

type errorContainer struct {
	date time.Time
	err  error
}

// NewBulkError creates empty BulkError struct definition
func NewBulkError() *BulkError {
	return &BulkError{
		innerErrors: make([]*errorContainer, 0),
		mutex:       &sync.Mutex{},
	}
}

// Add adds error in list
func (b *BulkError) Add(err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.innerErrors = append(b.innerErrors, &errorContainer{
		date: time.Now(),
		err:  err,
	})
}

// HasError checks if any error is added to the list
func (b *BulkError) HasError() bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return len(b.innerErrors) > 0
}

func (b *BulkError) ContainsType(err error) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for _, ec := range b.innerErrors {
		if reflect.TypeOf(ec.err) == reflect.TypeOf(err) {
			return true
		}
	}
	return false
}

func (b *BulkError) Count() int {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return len(b.innerErrors)
}

func (b *BulkError) Error() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	errs := ""
	for _, container := range b.innerErrors {
		if len(errs) > 0 {
			errs = fmt.Sprintf("%s\n", errs)
		}
		errs = fmt.Sprintf("%s[%s] %s", errs, container.date.Format(time.RFC3339), container.err)
	}
	return errs
}

var _ error = &BulkError{}
