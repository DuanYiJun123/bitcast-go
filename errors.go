package bitcast_go

import "errors"

var (
	ErrKeyIsEmpty            = errors.New("the key is empty")
	ErrIndexUpdateFailed     = errors.New("failed to update index")
	ErrKeyNotFound           = errors.New("key not found in database")
	ErrDataFileNotFound      = errors.New("data file is not found")
	ErrDataDirectoryCorrupte = errors.New("the database directory maybe corrupted")
	ErrInvalidCRC            = errors.New("invalid crc value,log record maybe corrupted")
)
