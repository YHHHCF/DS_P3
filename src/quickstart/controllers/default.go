package controllers

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"github.com/astaxie/beego"
	"io"
)

type MainController struct {
	beego.Controller
}



/*
	get request
*/
func (main *MainController) Get() {
	queryContent := main.GetString("cc")
	queryId := main.GetString(":queryId")
	// check url content
	if queryContent == "" || queryId != "q1" {
		main.URLError()
	} else {
		main.requestHandler(queryContent) // TODO
	}
}


func (main *MainController) requestHandler(queryContent string) {
	requestContent := parseRequest(queryContent)

	beego.Debug(requestContent.NewTx)
	if !checkBlcokChain(&requestContent) {
		main.URLError()
	}
}

/*
	parse the request content into block chain struct
 */
func parseRequest(request string) ChainRequest{
	// url base 64 decoder
	urlDecodeRes, _ := base64.URLEncoding.DecodeString(request)
	// Zlib uncompress
	uncompressedRes  := DoZlibUnCompress(urlDecodeRes)

	beego.Debug(string(uncompressedRes))
	// get the request content as struct
	var requestContent ChainRequest
	err := json.Unmarshal(uncompressedRes, &requestContent)
	if err != nil {
		beego.Debug(err)
	}

	beego.Debug("request content =-----")
	beego.Debug(requestContent)

	return requestContent
}

/*
	Zlib uncompress
 */
func DoZlibUnCompress(compressSrc []byte) []byte {
	b := bytes.NewReader(compressSrc)
	var out bytes.Buffer
	r, _ := zlib.NewReader(b)
	beego.Debug(r)
	io.Copy(&out, r)
	return out.Bytes()
}

/*
	find malicious request content
 */
func (main *MainController) URLError(){
	main.Ctx.WriteString("TEAMID,TEAM_AWS_ACCOUNT_ID\n<15619|TeamProject>")
	return
}

