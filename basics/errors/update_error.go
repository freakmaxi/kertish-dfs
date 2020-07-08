package errors

type UploadError struct {
	message string
}

func NewUploadError(message string) error {
	return &UploadError{
		message: message,
	}
}

func (u *UploadError) Error() string {
	return u.message
}

var _ error = &UploadError{}
