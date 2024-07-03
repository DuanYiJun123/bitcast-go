package data

import (
	"bitcast-go/fio"
	"bitcast-go/selferror"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"
const HintFileName = "hint-index"
const MergeFinishedFileName = "merge-finished"
const SeqNoFileName = "seq-no"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        //文件id
	WriteOff  int64         //文件写入到了哪个位置
	IoManager fio.IOManager //io 读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId)
}

//打开Hint索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

//OpenMergeFinishedFile 打开标识Merge完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0)
}

// OpenSeqNoFile 打开事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	return fileName
}

func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
	//初始化IOManager 管理器接口
	ioManager, err := fio.NewIoManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord 根据offset 从数据文件中读取LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	//拿到当前文件的大小
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	//如果offset+header最大的长度，已经超过了当前文件的长度，则只需要读取到文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	//读取Header信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	//拿到header以后，进行一个解码
	header, headerSize := decodeRecordHeader(headerBuf)
	//下面的两个条件表示读取到了文件的末尾，直接返回EOF错误
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.vauleSize == 0 {
		return nil, 0, io.EOF
	}

	//取出对应key和value的长度
	keySize, valueSize := int64(header.keySize), int64(header.vauleSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{
		Type: header.recordType,
	}

	//开始读取用户实际存储的key/value数据
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		//解出key和value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	//校验数据的CRC是否正确
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, selferror.ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

// Sync 持久化方法
func (df *DataFile) Sync() error {
	err := df.IoManager.Sync()
	return err
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) Write(buf []byte) error {
	_, err := df.IoManager.Write(buf)
	return err
}

//写入索引信息到hint文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos), //对pos进行编码
	}
	//再对record进行编码，然后写入
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

//指定读xx个字节，并指定使用IoManager，返回该字节数组
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}

//logRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        //crc校验值
	recordType LogRecordType //标识LogRecord的类型
	keySize    uint32        //key的长度
	vauleSize  uint32        //value的长度
}

//对字节数组中个Header进行解码，并拿到header信息
func decodeRecordHeader(buf []byte) (*logRecordHeader, int64) {
	header, i := decodeLogRecordHeader(buf)
	return header, i
}
