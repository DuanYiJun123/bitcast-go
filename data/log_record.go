package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTnxFinished
)

//crc type keySize valueSize

//4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

//LogRecordPos 数据存储索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件id，表示存储在了哪个文件中
	Offset int64  //表示数据在文件中的具体位置
}

// LogRecord 写入到数据文件的记录
//之所以叫日志，是因为数据文件中的数据是追加写入的。类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType //枚举，用于记录数据的状态
}

// EncodeLogRecord 对LogRecord进行编码，返回字节数组以及长度（需要对header信息编码为字节数组，因为key和value本身就是字节数组，无需编解码）
// crc校验值 / type类型 / key size / value size / key / value
//    4字节      1字节      变长（最大5）           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	//初始化一个头部信息的header字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	//从第五个字节存储type
	header[4] = logRecord.Type
	var index = 5
	//5字节之后，存储的是key和value的长度信息
	//使用变长类型，节省空间
	//binary.PutVarint函数会返回写入了多少个字节
	//binary.PutVarint函数是用于将整数编码为可变长度字节序列的函数，可变长度字节序列是一种用于压缩整数的编码方式，它使用更少的字节来表示较小的整数，从而节省存储空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value) //编码之后的长度就是header的长度+key长度+value长度
	encBytes := make([]byte, size)

	//将header部分内容拷贝过来
	copy(encBytes[:index], header[:index])
	//将Key和value数据拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	//对整个LogRecord的数据进行crc校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	//将crc的结果以小端字节序写入切片（一般arm或者x86的架构都是支持小端序的）
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5

	//Varint进行解码，返回长度和解码值，取出实际的 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	//取出实际的value size
	valueSize, n := binary.Varint(buf[index:])
	header.vauleSize = uint32(valueSize)
	index += n
	return header, int64(index)
}

//暂存的事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}
