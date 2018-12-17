package scheduler

import (
	"../analyz"
	"../base"
	"../download"
	"../middleware"
	"../pipeline"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var logger *logging.Logger = base.NewLogger()

const (
	DOWNLOADER_CODE   = "downloader"
	ANALYZER_CODE     = "analyzer"
	ITEMPIPELINE_CODE = "item_pipeline"
	SCHEDULER_CODE    = "scheduler"
)

type Scheduler interface {
	Start(channelLen uint,
		poolSize uint32,
		crawDepth uint32,
		httpClientGenerator GenHttpClient,
		respParsers []analyz.ParseResponse,
		itemProcessors []pipeline.ProcessItem,
		firstHttpReq *http.Request) (error error)
	Running() bool
	Stop() bool
	ErrorChan() <-chan error
	// 判断所有的处理模块都处于空闲模式
	Idle() bool
	// 获取摘要信息
	Summary(prefix string) SchedSummary
}

type GenHttpClient func() *http.Client

type goScheduler struct {
	poolSize      uint32
	channelLen    uint
	crawlDepth    uint32
	primaryDomain string

	channelManager middleware.ChannelManager
	stopSign       middleware.StopSign

	downloaderPool download.PageDownloaderPool
	analyzerPool   analyz.AnalyzerPool
	itemPipeline   pipeline.ItemPipeline

	running uint32 //0 未运行 1 已运行 2 已停止

	reqCache requestCache //请求缓存
	urlMap   map[string]bool
}

func NewScheduler() Scheduler {
	return &goScheduler{}
}

func (scheduler *goScheduler) Start(channelLen uint, poolSize uint32,
	crawDepth uint32, httpClientGenerator GenHttpClient,
	respParsers []analyz.ParseResponse, itemProcessors []pipeline.ProcessItem, firstHttpReq *http.Request) (err error) {
	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Fatal Scheduler Error:%s\n", p)
			logger.Fatal(errMsg)
			err = errors.New(errMsg)
		}
	}()

	if atomic.LoadUint32(&scheduler.running) == 1 {
		return errors.New("the scheduler has been started!\n")
	}
	atomic.StoreUint32(&scheduler.running, 1)

	if channelLen == 0 {
		return errors.New("the channel max length (capacity) can not be 0\n")
	}

	scheduler.channelLen = channelLen

	if poolSize == 0 {
		return errors.New("the pool size can not be 0!\n")
	}
	scheduler.poolSize = poolSize
	scheduler.crawlDepth = crawDepth

	scheduler.channelManager = generateChannelManager(scheduler.channelLen)

	if httpClientGenerator == nil {
		return errors.New("the http client  generator list is invalid")
	}

	downloadPool, err := generatePageDownloaderPool(scheduler.poolSize, httpClientGenerator)

	if err != nil {
		errMsg := fmt.Sprintf("Occur error when get page downloader pool:%s\n", err)
		return errors.New(errMsg)
	}

	scheduler.downloaderPool = downloadPool

	analyzerPool, err := generateAnalyzerPool(poolSize)

	if err != nil {
		errMsg := fmt.Sprintf("Occur error when get analyzer pool:%s\n", err)
		return errors.New(errMsg)
	}

	scheduler.analyzerPool = analyzerPool

	if itemProcessors == nil {
		return errors.New("the item processor list is invalid")
	}

	for i, ip := range itemProcessors {
		if ip == nil {
			return errors.New(fmt.Sprintf("the %dth item processor is invalid!", i))
		}
	}

	scheduler.itemPipeline = generateItemPipeLine(itemProcessors)

	if scheduler.stopSign == nil {
		scheduler.stopSign = middleware.NewStopSign()
	} else {
		scheduler.stopSign.Reset()
	}

	scheduler.urlMap = make(map[string]bool)

	if firstHttpReq == nil {
		return errors.New("the first http request is invalid")
	}

	scheduler.startDownloading()
	scheduler.activateAnalyzers(respParsers)
	scheduler.openItemPipeline()
	scheduler.schedule(10 * time.Millisecond)

	domain, err := getPrimaryDomain(firstHttpReq.Host)
	if err != nil {
		return err
	}
	scheduler.primaryDomain = domain

	firstReq := base.NewRequest(firstHttpReq, 0)

	scheduler.reqCache.put(firstReq)

	return nil
}

func (scheduler *goScheduler) activateAnalyzers(respParsers []analyz.ParseResponse) {

	go func() {
		for {
			resp, ok := <-scheduler.getRespChan()
			if !ok {
				break
			}
			go scheduler.analyze(respParsers, resp)
		}
	}()
}

func (scheduler *goScheduler) schedule(interval time.Duration) {
	go func() {
		for {

			if scheduler.stopSign.Signed() {
				scheduler.stopSign.Deal(SCHEDULER_CODE)
				return
			}

			remainder := cap(scheduler.getReqChan()) - len(scheduler.getReqChan())

			var temp *base.Request

			for remainder > 0 {
				temp = scheduler.reqCache.get()
				if temp == nil {
					break
				}
				scheduler.getReqChan() <- *temp
				remainder--
				time.Sleep(interval)
			}
		}
	}()
}

func (scheduler *goScheduler) Stop() bool {
	if atomic.LoadUint32(&scheduler.running) != 1 {
		return false
	}

	scheduler.stopSign.Sign()

	scheduler.channelManager.Close()
	scheduler.reqCache.close()
	atomic.StoreUint32(&scheduler.running, 2)
	return true
}
func (scheduler *goScheduler) Running() bool {
	return atomic.LoadUint32(&scheduler.running) == 1
}

func (scheduler *goScheduler) Idle() bool {
	idleDownloadPool := scheduler.downloaderPool.Used() == 0
	idleAnalyzerPool := scheduler.analyzerPool.Used() == 0
	idleItemPipeline := scheduler.itemPipeline.ProcessingNumber() == 0
	return idleAnalyzerPool && idleDownloadPool && idleItemPipeline
}

func (scheduler *goScheduler) ErrorChan() <-chan error {
	if scheduler.channelManager.Status() != middleware.CHANNEL_MANAGER_STATUS_INITIALIZED {
		return nil
	}

	return scheduler.getErrorChan()
}

func (scheduler *goScheduler) analyze(respParsers []analyz.ParseResponse, resp base.Response) {
	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Analyzer Error:%s\n", p)
			logger.Fatal(errMsg)
		}
	}()

	analyzer, err := scheduler.analyzerPool.Take()
	if err != nil {
		errMsg := fmt.Sprintf("Analyzer Pool Take Fail")
		scheduler.sendError(errors.New(errMsg), SCHEDULER_CODE)
		return
	}

	defer func() {
		err := scheduler.analyzerPool.Return(analyzer)
		if err != nil {
			errMsg := fmt.Sprintf("Analyzer pool error:%s", err)
			scheduler.sendError(errors.New(errMsg), SCHEDULER_CODE)
		}
	}()

	code := generateCode(ANALYZER_CODE, analyzer.Id())

	dataList, errs := analyzer.Analyzer(respParsers, resp)

	if dataList != nil {
		for _, data := range dataList {

			if data == nil {
				continue
			}

			switch d := data.(type) {
			case *base.Request:
				scheduler.saveReqToCache(*d, code)
			case *base.Item:
				scheduler.sendItem(*d, code)
			default:
				errMsg := fmt.Sprintf("unsupported data type:%T !(value=%v)", d, d)
				scheduler.sendError(errors.New(errMsg), code)
			}
		}
	}
	if errs != nil {
		for _, err := range errs {
			scheduler.sendError(err, code)
		}
	}
}

func (scheduler *goScheduler) saveReqToCache(req base.Request, code string) bool {
	httpReq := req.HttpReq()
	if httpReq == nil {
		logger.Warning("Ignore the request! It is HTTP request is invalid\n")
		return false
	}

	if httpReq.URL == nil {
		logger.Warningf("Ignore the request!It it is url is invalid:%s\n", httpReq.URL)
	}

	if strings.ToLower(httpReq.URL.Scheme) != "http" {
		logger.Warningf("Ignore the request!It is url schema '%s', but should be http.\n", httpReq.URL.Scheme)
	}

	if _, ok := scheduler.urlMap[httpReq.URL.String()]; ok {
		logger.Warningf("Ignore the request! it is url is repeated.(requestUrl=%s)\n", httpReq.URL)
		return false
	}

	if pd, _ := getPrimaryDomain(httpReq.URL.String()); pd != scheduler.primaryDomain {

		logger.Warningf("Ignore the request! It's host '%s' not in primary domain '%s'.(requestUrl=%s)\n", httpReq.Host, scheduler.primaryDomain, httpReq.URL)
		return false
	}

	if req.Depth() > scheduler.crawlDepth {
		logger.Warningf("Ignore the request! It is depth %d greater than %d.(requestUrl=%s)\n", req.Depth(), scheduler.crawlDepth, httpReq.URL)
		return false
	}

	if scheduler.stopSign.Signed() {
		scheduler.stopSign.Deal(code)
		return false
	}

	scheduler.reqCache.put(&req)
	scheduler.urlMap[httpReq.URL.String()] = true
	return true
}

func (scheduler *goScheduler) openItemPipeline() {

	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Fatal Item Processing Error:%s\n", p)
			logger.Fatal(errMsg)
		}
	}()

	go func() {
		scheduler.itemPipeline.SetFailFast(true)

		code := ITEMPIPELINE_CODE

		for item := range scheduler.getItemChan() {

			go func(item base.Item) {
				errs := scheduler.itemPipeline.Send(item)

				if errs != nil {
					for _, err := range errs {
						scheduler.sendError(err, code)
					}
				}
			}(item)
		}
	}()
}

func (scheduler *goScheduler) startDownloading() {
	go func() {
		for {
			req, ok := <-scheduler.getReqChan()
			if !ok {
				break
			}
			go scheduler.download(req)
		}
	}()
}

func (scheduler *goScheduler) download(request base.Request) {
	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Fatal Download Error:%s\n", p)
			logger.Fatal(errMsg)
		}
	}()
	downloader, err := scheduler.downloaderPool.Take()

	if err != nil {
		errMsg := fmt.Sprintf("Downloader Pool Take fail")
		scheduler.sendError(errors.New(errMsg), SCHEDULER_CODE)
		return
	}
	defer func() {
		err := scheduler.downloaderPool.Return(downloader)
		if err != nil {
			errMsg := fmt.Sprintf("Downloader pool error:%s", err)
			scheduler.sendError(errors.New(errMsg), SCHEDULER_CODE)
		}
	}()

	code := generateCode(DOWNLOADER_CODE, downloader.Id())

	resp, err := downloader.Download(request)

	if resp != nil {
		scheduler.sendResp(*resp, code)
	}
	if err != nil {
		scheduler.sendError(err, code)
	}
}

func (scheduler *goScheduler) getReqChan() chan base.Request {
	reqChan, err := scheduler.channelManager.ReqChan()
	if err != nil {
		panic(err)
	}
	return reqChan
}

func (scheduler *goScheduler) sendResp(resp base.Response, code string) bool {

	if scheduler.stopSign.Signed() {
		scheduler.stopSign.Deal(code)
		return false
	}
	scheduler.getRespChan() <- resp
	return true
}

func (scheduler *goScheduler) sendItem(item base.Item, code string) bool {

	if scheduler.stopSign.Signed() {
		scheduler.stopSign.Deal(code)
		return false
	}

	scheduler.getItemChan() <- item
	return true
}

func (scheduler *goScheduler) sendError(err error, code string) bool {

	if err != nil {
		return false
	}

	codePrefix := parseCode(code)[0]

	var errorType base.ErrorType

	switch codePrefix {
	case DOWNLOADER_CODE:
		errorType = base.DOWNLOAD_ERROR
	case ANALYZER_CODE:
		errorType = base.ANALYZER_ERROR
	case ITEMPIPELINE_CODE:
		errorType = base.ITEM_PROCESSOR_ERROR
	}

	crawlerError := base.NewCrawlerErrorInfo(errorType, err.Error())

	if scheduler.stopSign.Signed() {
		scheduler.stopSign.Deal(code)
		return false
	}

	go func() {
		scheduler.getErrorChan() <- crawlerError
	}()
	return true
}

func (scheduler *goScheduler) getErrorChan() chan error {
	errChan, err := scheduler.channelManager.ErrorChan()
	if err != nil {
		panic(err)
	}
	return errChan
}

func (scheduler *goScheduler) getRespChan() chan base.Response {
	respChan, err := scheduler.channelManager.RespChan()
	if err != nil {
		panic(err)
	}
	return respChan
}

func (scheduler *goScheduler) getItemChan() chan base.Item {
	itemChan, err := scheduler.channelManager.ItemChan()
	if err != nil {
		panic(err)
	}
	return itemChan
}

func (scheduler *goScheduler) Summary(prefix string) SchedSummary {
	return NewSchedSummary(scheduler, prefix)
}

func getPrimaryDomain(host string) (string, error) {

	return host, nil
}

func generateItemPipeLine(itemProcessors []pipeline.ProcessItem) pipeline.ItemPipeline {
	return pipeline.NewItemPipeLine(itemProcessors)
}

func generateAnalyzerPool(poolSize uint32) (analyz.AnalyzerPool, error) {

	return analyz.NewAnalyzerPool(poolSize, func() analyz.Analyzer {
		return analyz.NewAnalyzer()
	})

}

func generateChannelManager(channelLen uint) middleware.ChannelManager {
	return middleware.NewChannelManager(channelLen)
}

func generatePageDownloaderPool(poolSize uint32, httpClientGenerator GenHttpClient) (download.PageDownloaderPool, error) {
	pool, err := download.NewPageDownloaderPool(poolSize, func() download.PageDownloader {
		return download.NewPageDownloader(httpClientGenerator())
	})

	if err != nil {
		return nil, err
	}
	return pool, nil
}

func generateCode(prefix string, id uint32) string {
	return fmt.Sprintf("%s-%d", prefix, id)
}

func parseCode(code string) []string {
	result := make([]string, 2)
	var codePrefix string
	var id string
	index := strings.Index(code, "-")
	if index > 0 {
		codePrefix = code[:index]
		id = code[index+1:]
	} else {
		codePrefix = code
	}
	result[0] = codePrefix
	result[1] = id
	return result
}
