package bitcast_go

type Options struct {
	DirPath string //数据库数据目录

	//数据文件的大小阈值
	DataFileSize int64

	//每次写入数据是否持久化
	SyncWrites bool
}
