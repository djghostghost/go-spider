package download

import (
	"../base"
	"../middleware"
	"net/http"
)

type PageDownloader interface {
	Id() uint32
	Download(req base.Request) (*base.Response, error)
}

var downloaderIdGenerator middleware.IdGenerator=middleware.NewIdGenerator()

func genDownloaderId() uint32{
	return downloaderIdGenerator.GetUint32()
}
type goPageDownloader struct{

	httpClient *http.Client
	id uint32


}
func NewPageDownloader(client *http.Client) PageDownloader{

	id := genDownloaderId()
	if client == nil{
		client=&http.Client{}
	}

	return &goPageDownloader{httpClient:client,id:id}
}


func (downloader *goPageDownloader) Id() uint32{
	return downloader.id
}

func (downloader *goPageDownloader) Download(req base.Request)(*base.Response,error){
	httpReq := req.HttpReq()

	httpResp,err := downloader.httpClient.Do(httpReq)
	if err!=nil{
		return nil,err
	}
	return base.NewResponse(httpResp,req.Depth()),nil
}


