package bitcast_go

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb.Put([]byte("test"), []byte("123"))
	wb.Put([]byte("test1"), []byte("1234"))
	wb.Put([]byte("test2"), []byte("12345"))
	wb.Put([]byte("test3"), []byte("123456"))

	val, err := db.Get([]byte("test"))
	t.Log(val)
	t.Log(err)

	//正常提交
	wb.Commit()
	get, err := db.Get([]byte("test"))
	t.Log(get)
	t.Log(err)
}
