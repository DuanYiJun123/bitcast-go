package selferror

import "errors"

var (
	ErrKeyIsEmpty            = errors.New("the key is empty")
	ErrIndexUpdateFailed     = errors.New("failed to update index")
	ErrKeyNotFound           = errors.New("key not found in database")
	ErrDataFileNotFound      = errors.New("data file is not found")
	ErrDataDirectoryCorrupte = errors.New("the database directory maybe corrupted")
	ErrInvalidCRC            = errors.New("invalid crc value,log record maybe corrupted")
	ErrExceedMaxBatchNum     = errors.New("exceed the max batch")
	ErrMergeIsProgress       = errors.New("merge is in progress,try again later")
	ErrDatabaseIsUsing       = errors.New("the database directory is used")
	ErrMergeRatioUnreached   = errors.New("the merge ratio do not reach the ratio")
	ErrNoEnoughSpaceForMerge = errors.New("no enougn space for merge")
)
