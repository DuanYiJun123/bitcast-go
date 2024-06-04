package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 222)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	//只有一条LogRecord

	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}

	res1, size1 := EncodeLogRecord(rec1)
	dataFile.Write(res1)
	assert.Nil(t, err)

	readRec1, readSize1, err := dataFile.ReadLogRecord(0)

	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)

	//多条 LogRecord，从不同的位置读取
	//rec2 := &LogRecord{
	//	Key:   []byte("name"),
	//	Value: []byte("a new value"),
	//}
	//res2, size2 := EncodeLogRecord(rec2)
	//dataFile.Write(res2)
	//assert.Nil(t, err)
	//t.Log(size2)
}
