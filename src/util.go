package main

import (
	"bufio"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"io"
	"os"
	"reflect"
	"sort"
	"unsafe"
)

func Md5(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}

func Encrypt(message string) string {

	//BASE64Table := "IJjkKLMNO567PQX12RVW3YZaDEFGbcdefghiABCHlSTUmnopqrxyz04stuvw89+/"
	BASE64Table := "3YZaDEFGbcdeKLMNyzfghiIJjk04stuO567PQX12RVWABCHlSTUmnopqrxvw89+/"

	content := *(*[]byte)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&message))))
	coder := base64.NewEncoding(BASE64Table)
	return coder.EncodeToString(content)

}

func Decrypt(message string) string {

	//BASE64Table := "IJjkKLMNO567PQX12RVW3YZaDEFGbcdefghiABCHlSTUmnopqrxyz04stuvw89+/"
	BASE64Table := "3YZaDEFGbcdeKLMNyzfghiIJjk04stuO567PQX12RVWABCHlSTUmnopqrxvw89+/"

	coder := base64.NewEncoding(BASE64Table)
	result, _ := coder.DecodeString(message)
	return *(*string)(unsafe.Pointer(&result))

}

func Readfile(filename string) string {

	file, err := os.Open(filename)
	if err != nil {
		return ""
	}

	defer file.Close()

	text := ""
	line := bufio.NewReader(file)
	for {
		content, _, err := line.ReadLine()
		if err == io.EOF {
			break
		}
		text = text + string(content)
	}

	return text
}

func Writefile(filename string, text string, mode string) {

	if mode == "a" {
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0666)

		if err != nil {
			return
		}
		defer file.Close()

		writer := bufio.NewWriter(file)

		writer.WriteString(text)

		writer.Flush()

	} else if mode == "w" {
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)

		if err != nil {
			return
		}
		defer file.Close()

		writer := bufio.NewWriter(file)

		writer.WriteString(text)

		writer.Flush()

	} else {
		return
	}

}

func Existfile(filename string) bool {
	_, err := os.Lstat(filename)
	return !os.IsNotExist(err)
}

func Filesize(filename string) int64 {
	fi, err := os.Stat(filename)
	if err == nil {
		return fi.Size()
	}
	return -1
}

func Removefile(filename string) bool {

	err := os.Remove(filename)

	if err != nil {
		return false

	}
	return true
}

var datapath = Data_dir + "taskcache/"

func Makedirforhash(hash string) {
	filedir := datapath + hash[0:2] + "/" + hash[2:4] + "/"
	os.MkdirAll(filedir, os.ModePerm)
}

func Filedirfromhash(hash string) string {
	filedir := datapath + hash[0:2] + "/" + hash[2:4] + "/"
	return filedir
}

type MapsSort struct {
	Key     string
	MapList []map[string]interface{}
}

func (m *MapsSort) Len() int {
	return len(m.MapList)
}

func (m *MapsSort) Less(i, j int) bool {
	return m.MapList[i][m.Key].(int) > m.MapList[j][m.Key].(int)
}

func (m *MapsSort) Swap(i, j int) {
	m.MapList[i], m.MapList[j] = m.MapList[j], m.MapList[i]
}

func Sort(key string, maps []map[string]interface{}) []map[string]interface{} {
	mapsSort := MapsSort{}
	mapsSort.Key = key
	mapsSort.MapList = maps
	sort.Sort(&mapsSort)

	return mapsSort.MapList
}
