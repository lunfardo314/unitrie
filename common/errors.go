package common

import (
	"errors"
)

var (
	ErrNotAllBytesConsumed = errors.New("serialization error: not all bytes were consumed")

	// ErrDBUnavailable implementations of KV storage may choose to panic with this error in case the
	// underlying storage is closed or unavailable
	ErrDBUnavailable = errors.New("database is closed or unavailable")
)
