package analyz

import (
	"../base"
	"../middleware"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"net/http"
)

type ParseResponse func(httpResp *http.Response, respDepth uint32) ([]base.Data, []error)

type Analyzer interface {
	Id() uint32
	Analyzer(respParsers []ParseResponse, resp base.Response) ([]base.Data, []error)
}



type goAnalyzer struct {
	id uint32
}

var genAnalyzerId middleware.IdGenerator = middleware.NewIdGenerator()

func generateAnalyzerId() uint32 {
	return genAnalyzerId.GetUint32()
}

func NewAnalyzer() Analyzer {

	return &goAnalyzer{id: generateAnalyzerId()}
}

func (analyzer *goAnalyzer) Id() uint32 {
	return analyzer.id
}

var logger *logging.Logger = base.NewLogger()

func (analyzer *goAnalyzer) Analyzer(respParsers []ParseResponse, resp base.Response) ([]base.Data, []error) {

	if respParsers == nil {
		err := errors.New("the response parser list is invalid")
		return nil, []error{err}
	}

	httpResp := resp.HttpResp()

	if httpResp == nil {
		err := errors.New("the http response is invalid")
		return nil, []error{err}
	}

	reqUrl := httpResp.Request.URL
	logger.Info("parse the response (reqUrl=%s)..\n", reqUrl)
	respDepth := resp.Depth()

	dataList := make([]base.Data, 0)
	errorList := make([]error, 0)

	for i, respParser := range respParsers {

		if respParser == nil {
			err := errors.New(fmt.Sprintf("the document parser [%d] is invalid", i))
			errorList = append(errorList, err)
			continue
		}
		pDataList, pErrorList := respParser(httpResp, respDepth)

		if pDataList!= nil{

			for _,pData := range pDataList{
				dataList=appendDataList(dataList,pData,respDepth)
			}
		}

		if pErrorList!=nil{
			for _,pError := range pErrorList{
				errorList=appendErrorList(errorList,pError)
			}

		}
	}

	return dataList,errorList
}

func appendDataList(dataList []base.Data, data base.Data, respDepth uint32) []base.Data {
	if data == nil {
		return dataList
	}

	req, ok := data.(*base.Request)

	if !ok {
		return append(dataList, data)
	}
	//TODO ???????
	newDepth := respDepth + 1
	if req.Depth() != newDepth {
		req = base.NewRequest(req.HttpReq(), newDepth)
	}
	return append(dataList, req)
}

func appendErrorList(errorList []error, err error) []error {
	if err == nil {
		return errorList
	}
	return append(errorList, err)
}
