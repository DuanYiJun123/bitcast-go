package index

import (
	"bitcast-go/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

//B+树索引 主要封装了go.etcd.io/bbolt库

type BPlusTree struct {
	tree *bbolt.DB //支持并发访问，无需再加锁了
}

func NewBPlusTree(dirPath string) *BPlusTree {
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, nil)
	if err != nil {
		panic("failed to open bptree")
	}

	//创建对应的Bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("faild to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

//向索引中存储key 对应的数据位置信息
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return true
}

//根据key 取出对应的索引位置信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

//根据Key 删除对应的位置信息
func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return ok
}

//索引中存在的数据量
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return size
}

//
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return nil
}
