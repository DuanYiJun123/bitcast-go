package bitcast_go

import (
	"bitcast-go/data"
	"bitcast-go/index"
	"sync"
)

//DB bitcask存储引擎实例
type DB struct {
	option     Options
	mu         *sync.RWMutex
	activeFile *data.DataFile            //当前活跃数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile //旧的数据文件，只能用于读
	index      index.Indexer             //内存索引
}

//写入Key/Value 数据 key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	//判断key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//构造LogRecord结构体
	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecord(log_record)
	if err != nil {
		return err
	}
	//更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	//判断key的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	//从内存数据中取出key对应的索引信息
	logRecordPos := db.index.Get(key)

	//如果key不存在内存索引中，说明key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	//根据文件id找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	//数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//找到了对应的数据文件，并根据偏移量读取数据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

//追加写数据到活跃文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	//判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	//如果为空，则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	//写入数据编码
	enRecord, size := data.EncodeLogRecord(record)
	//如果写入的数据已经到达了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.option.DataFileSize {
		//先持久化数据文件，保证已有的数据持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		//打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(enRecord); err != nil {
		return nil, err
	}
	//根据用户配置决定是否持久化
	if db.option.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err

		}
	}

	//构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法前，必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	//打开新的数据文件
	dataFile, err := data.OpenDataFile(db.option.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
