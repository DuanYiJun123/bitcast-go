package main

import (
	bitcast_go "bitcast-go"
	"bitcast-go/selferror"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *bitcast_go.DB

func init() {
	//初始化db实例
	var err error
	options := bitcast_go.DefaultOptions
	dir, _ := os.MkdirTemp("/Users/yijun.dyj/GolandProjects/bitcast-go", "bitcask-go-http")
	options.DirPath = dir
	db, err = bitcast_go.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Println("failed to put value in db %v\n", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil && err != selferror.ErrKeyIsEmpty {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Println("failed to get value in db: %v\n", err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))

}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil && err != selferror.ErrKeyIsEmpty {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Println("failed to get value in db: %v\n", err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, keys := range keys {
		result = append(result, string(keys))
	}
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	//注册处理方法
	http.HandleFunc("/bitcask/put", handlePut)

	http.HandleFunc("/bitcask/get", handleGet)

	http.HandleFunc("/bitcask/delete", handleDelete)

	http.HandleFunc("/bitcask/listKeys", handleListKeys)

	http.HandleFunc("/bitcask/stat", handleStat)
	//启动http服务
	http.ListenAndServe("localhost:8080", nil)
}
