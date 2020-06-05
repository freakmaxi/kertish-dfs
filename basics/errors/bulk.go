package errors

import (
	"fmt"
	"time"
)

type BulkError struct {
	innerErrors []*errorContainer
}

type errorContainer struct {
	date time.Time
	err  error
}

func NewBulkError() *BulkError {
	return &BulkError{
		innerErrors: make([]*errorContainer, 0),
	}
}

func (b *BulkError) Add(err error) {
	b.innerErrors = append(b.innerErrors, &errorContainer{
		date: time.Now(),
		err:  err,
	})
}

func (b *BulkError) HasError() bool {
	return len(b.innerErrors) > 0
}

func (b *BulkError) Error() string {
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
