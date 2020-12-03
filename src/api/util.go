package main


import "crypto/md5"
import "encoding/hex"
import "encoding/base64"
import "unsafe"
import "io"
import "os"
import "bufio"
import "reflect"

/*
def redirect(url):
	
	return HttpResponseRedirect(url)

def response(code, msg, data):
	
	data = {"code":code, "msg": msg, "data": data}
	
	resp = HttpResponse(json.dumps(data), content_type='application/json')
	resp["Access-Control-Allow-Origin"] = "*"
	resp["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
	resp["Access-Control-Max-Age"] = "1800"
	resp["Access-Control-Allow-Headers"] = "*"
	
	return resp

def responsefile(filename):
	
	downloadname = filename[filename.rfind('/')+1:]
	
	response = FileResponse(open(filename ,'rb'))
	response['Content-Type']='application/octet-stream'
	response['Content-Disposition']='attachment;filename="'+downloadname+'"'
	
	return response
*/


func Md5(str string) string  {
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

func Decrypt (message string) string {

	//BASE64Table := "IJjkKLMNO567PQX12RVW3YZaDEFGbcdefghiABCHlSTUmnopqrxyz04stuvw89+/"
	BASE64Table := "3YZaDEFGbcdeKLMNyzfghiIJjk04stuO567PQX12RVWABCHlSTUmnopqrxvw89+/"
	
	coder := base64.NewEncoding(BASE64Table)
    result, _ := coder.DecodeString(message)
    return *(*string)(unsafe.Pointer(&result))
	
}



func Readfile(filename string) string{

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

	}else if mode == "w" {
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)

		if err != nil {
			return
		}
		defer file.Close()
	
		writer := bufio.NewWriter(file)
		
		writer.WriteString(text)
	
		writer.Flush()

	}else{
		return
	}

   

}

func Existfile(filename string) bool {
	_, err := os.Lstat(filename)
	return !os.IsNotExist(err)
}

func Filesize(filename string) int64 {
    fi,err:=os.Stat(filename) 
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

func Makedirforhash(hash string){
	filedir := datapath + hash[0:2] + "/" +  hash[2:4] + "/";
	os.MkdirAll(filedir, os.ModePerm) 
}

func Filedirfromhash(hash string) string{
	filedir := datapath + hash[0:2] + "/" +  hash[2:4] + "/";
	return filedir;
}

