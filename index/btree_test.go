package index

import (
	"bitcast-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

//单元测试

func TestBtree_Put(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)
	res2 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 200,
	})
	assert.True(t, res2)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{
		Fid:    33,
		Offset: 33,
	})
	assert.True(t, res3)
	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)
}
