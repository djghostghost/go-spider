package base

import "bytes"

type ErrorType string
type CrawlerError interface {
	Type() ErrorType
	Error() string
}

type CrawlerErrorInfo struct {
	errType    ErrorType
	errMsg     string
	fullErrMsg string
}

const (
	DOWNLOAD_ERROR       ErrorType = "DownLoader ERROR"
	ANALYZER_ERROR       ErrorType = "Analyzer ERROR"
	ITEM_PROCESSOR_ERROR ErrorType = "Item Processor Error"
)

func NewCrawlerErrorInfo(errType ErrorType, errMsg string) *CrawlerErrorInfo {
	return &CrawlerErrorInfo{errType: errType, errMsg: errMsg}
}

func (errorInfo *CrawlerErrorInfo) Type() ErrorType {
	return errorInfo.errType
}

func (errorInfo *CrawlerErrorInfo) Error() string {

	if errorInfo.fullErrMsg != "" {
		errorInfo.getFullMsg()
	}

	return errorInfo.fullErrMsg
}

//生成错误信息
func (errorInfo *CrawlerErrorInfo) getFullMsg() {

	var buffer bytes.Buffer

	buffer.WriteString("Crawler Error:")
	if errorInfo.errType != "" {
		buffer.WriteString(string(errorInfo.errType))
		buffer.WriteString(":")
	}
	buffer.WriteString(errorInfo.errMsg)
	errorInfo.fullErrMsg = buffer.String()
}
