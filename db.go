package bitcast_go

import (
	"bitcast-go/data"
	"bitcast-go/index"
	"bitcast-go/selferror"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

//DB bitcast存储引擎实例
type DB struct {
	option     Options                   //配置信息
	mu         *sync.RWMutex             //锁
	fileIds    []int                     //仅用于加载索引的时候使用（因为在加载磁盘文件的时候，已经将文件Id取出，但是在olderFiles的map里面是无序的，所以这里需要复用一下这个ids）
	activeFile *data.DataFile            //当前活跃数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile //旧的数据文件，只能用于读
	index      index.Indexer             //内存索引
}

//Open 打开bitcask存储引擎实例
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	//判断数据目录是否存在，如果不存在的话，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//初始化DB实例结构体
	db := &DB{
		option:     options,
		mu:         new(sync.RWMutex),
		activeFile: nil,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexerType),
	}

	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}

//写入Key/Value 数据 key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	//判断key 是否有效
	if len(key) == 0 {
		return selferror.ErrKeyIsEmpty
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
		return selferror.ErrIndexUpdateFailed
	}
	return nil
}

// Delete 根据Key删除对应的数据
func (db *DB) Delete(key []byte) error {
	//判断key的有效性
	if len(key) == 0 {
		return selferror.ErrKeyIsEmpty
	}

	//先检查key是否存在，如果不存在的话直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	//构造logRecord，标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return nil
	}

	//从内存索引当中将对应的key删除
	ok := db.index.Delete(key)
	if !ok {
		return selferror.ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	//判断key的有效性
	if len(key) == 0 {
		return nil, selferror.ErrKeyIsEmpty
	}
	//从内存数据中取出key对应的索引信息
	logRecordPos := db.index.Get(key)

	//如果key不存在内存索引中，说明key不存在
	if logRecordPos == nil {
		return nil, selferror.ErrKeyNotFound
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
		return nil, selferror.ErrDataFileNotFound
	}

	//找到了对应的数据文件，并根据偏移量读取数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, selferror.ErrKeyNotFound
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

//从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.option.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	//遍历目录中的所有文件，找到所有以.data 结尾的文件

	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			//00001.data 进行分割，前面部分作为文件Id
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			//如果解析发生了错误，则数据目录存在其他文件，说明可能损坏掉了
			if err != nil {
				return selferror.ErrDataDirectoryCorrupte
			}
			fileIds = append(fileIds, fileId)
		}
	}

	//对文件ID进行排序，从小到大依次加载
	sort.Ints(fileIds)

	//赋值，为了后面加载索引的时候使用
	db.fileIds = fileIds

	//遍历每个文件Id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.option.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { //最后一个，id是最大的，说明是当前活跃的文件
			db.activeFile = dataFile
		} else { //说明是旧的数据文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

//从数据文件中加载索引
//遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	//没有文件，说明数据库为空，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}
	//遍历所有文件的id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				//如果是读完的情况，跳出循环，其他错误则直接返回
				if err == io.EOF {
					break
				}
				return err
			}
			//构造内存索引，并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			var ok bool
			if logRecord.Type == data.LogRecordDeleted {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, logRecordPos)
			}
			if !ok {
				return selferror.ErrIndexUpdateFailed
			}
			//递增offset，下一次从新的位置获取
			offset += size
		}

		//如果最后一个文件是当前活跃文件，更新这个文件的writeoff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}
