package scheduler

import (
	"bytes"
	"fmt"
)

// 调度器摘要信息的接口类型。
type SchedSummary interface {
	String() string               // 获得摘要信息的一般表示。
	Detail() string               // 获取摘要信息的详细表示。
	Same(other SchedSummary) bool // 判断是否与另一份摘要信息相同。
}

// 创建调度器摘要信息。
func NewSchedSummary(sched *goScheduler, prefix string) SchedSummary {
	if sched == nil {
		return nil
	}
	urlCount := len(sched.urlMap)
	var urlDetail string
	if urlCount > 0 {
		var buffer bytes.Buffer
		buffer.WriteByte('\n')
		for k, _ := range sched.urlMap {
			buffer.WriteString(prefix)
			buffer.WriteString(prefix)
			buffer.WriteString(k)
			buffer.WriteByte('\n')
		}
		urlDetail = buffer.String()
	} else {
		urlDetail = "\n"
	}
	return &goSchedSummary{
		prefix:              prefix,
		running:             sched.running,
		crawlDepth:          sched.crawlDepth,
		chanmanSummary:      sched.channelManager.Summary(),
		reqCacheSummary:     sched.reqCache.summary(),
		dlPoolLen:           sched.downloaderPool.Used(),
		dlPoolCap:           sched.downloaderPool.Total(),
		analyzerPoolLen:     sched.analyzerPool.Used(),
		analyzerPoolCap:     sched.analyzerPool.Total(),
		itemPipelineSummary: sched.itemPipeline.Summary(),
		urlCount:            urlCount,
		urlDetail:           urlDetail,
		stopSignSummary:     sched.stopSign.Summary(),
	}
}

// 调度器摘要信息的实现类型。
type goSchedSummary struct {
	prefix              string            // 前缀。
	running             uint32            // 运行标记。
	crawlDepth          uint32            // 爬取的最大深度。
	chanmanSummary      string            // 通道管理器的摘要信息。
	reqCacheSummary     string            // 请求缓存的摘要信息。
	dlPoolLen           uint32            // 网页下载器池的长度。
	dlPoolCap           uint32            // 网页下载器池的容量。
	analyzerPoolLen     uint32            // 分析器池的长度。
	analyzerPoolCap     uint32            // 分析器池的容量。
	itemPipelineSummary string            // 条目处理管道的摘要信息。
	urlCount            int               // 已请求的URL的计数。
	urlDetail           string            // 已请求的URL的详细信息。
	stopSignSummary     string            // 停止信号的摘要信息。
}

func (ss *goSchedSummary) String() string {
	return ss.Summary(false)
}

func (ss *goSchedSummary) Detail() string {
	return ss.Summary(true)
}

// 获取摘要信息。
func (ss *goSchedSummary) Summary(detail bool) string {
	prefix := ss.prefix
	template := prefix + "Running: %v \n" +
		prefix + "Crawl depth: %d \n" +
		prefix + "Channels manager: %s \n" +
		prefix + "Request cache: %s\n" +
		prefix + "Downloader pool: %d/%d\n" +
		prefix + "Analyzer pool: %d/%d\n" +
		prefix + "Item pipeline: %s\n" +
		prefix + "Urls(%d): %s" +
		prefix + "Stop sign: %s\n"
	return fmt.Sprintf(template,
		func() bool {
			return ss.running == 1
		}(),

		ss.crawlDepth,
		ss.chanmanSummary,
		ss.reqCacheSummary,
		ss.dlPoolLen, ss.dlPoolCap,
		ss.analyzerPoolLen, ss.analyzerPoolCap,
		ss.itemPipelineSummary,
		ss.urlCount,
		func() string {
			if detail {
				return ss.urlDetail
			} else {
				return "<concealed>\n"
			}
		}(),
		ss.stopSignSummary)
}

func (ss *goSchedSummary) Same(other SchedSummary) bool {
	if other == nil {
		return false
	}
	otherSs, ok := interface{}(other).(*goSchedSummary)
	if !ok {
		return false
	}
	if ss.running != otherSs.running ||
		ss.crawlDepth != otherSs.crawlDepth ||
		ss.dlPoolLen != otherSs.dlPoolLen ||
		ss.dlPoolCap != otherSs.dlPoolCap ||
		ss.analyzerPoolLen != otherSs.analyzerPoolLen ||
		ss.analyzerPoolCap != otherSs.analyzerPoolCap ||
		ss.urlCount != otherSs.urlCount ||
		ss.stopSignSummary != otherSs.stopSignSummary ||
		ss.reqCacheSummary != otherSs.reqCacheSummary ||
		ss.itemPipelineSummary != otherSs.itemPipelineSummary ||
		ss.chanmanSummary != otherSs.chanmanSummary {
		return false
	} else {
		return true
	}
}