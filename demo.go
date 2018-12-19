package main

import (
	"./analyz"
	"./base"
	"./pipeline"
	"./scheduler"
	"errors"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/op/go-logging"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func main() {

	channelArgs := base.NewChannelArgs(10, 10, 10, 10)
	poolBaseArgs := base.NewPoolBaseArgs(3, 3)
	crawlDepth := uint32(1)

	httpClientGenerator := genHttpClient

	respParsers := getResponseParsers()

	startUrl := "http://www.sogou.com"
	firstReq, err := http.NewRequest("GET", startUrl, nil)
	if err != nil {
		logging.Logger.Error(err)
		return
	}

	scheduler := scheduler.NewScheduler()
	scheduler.Start(channelArgs,
		poolBaseArgs,
		crawlDepth,
		httpClientGenerator,
		respParsers,
		getItemProcessors(),
		firstReq)

}

func genHttpClient() *http.Client {
	return &http.Client{}
}

func getItemProcessors() []pipeline.ProcessItem {

	itemProcessors := []pipeline.ProcessItem{
		processItem,
	}

	return itemProcessors
}

func getResponseParsers() []analyz.ParseResponse {
	parsers := []analyz.ParseResponse{
		parseForATag,
	}
	return parsers
}

func parseForATag(httpResp *http.Response, respDepth uint32) ([]base.Data, []error) {

	if httpResp.StatusCode != 200 {
		err := errors.New(fmt.Sprintf("Unsupported status code %d.(httpResponse=%v)", httpResp.StatusCode, httpResp))
		return nil, []error{err}
	}

	httpRespBody := httpResp.Body

	defer func() {
		if httpRespBody != nil {
			httpRespBody.Close()
		}
	}()

	dataList := make([]base.Data, 0)
	errs := make([]error, 0)

	doc, err := goquery.NewDocumentFromReader(httpRespBody)
	if err != nil {
		errs = append(errs, err)
		return dataList, errs
	}

	doc.Find("a").Each(func(index int, sel *goquery.Selection) {
		href, exists := sel.Attr("href")

		if !exists || href == "" || href == "#" || href == "/" {
			return
		}

		href = strings.TrimSpace(href)

		lowerHref := strings.ToLower(href)

		if href != "" && !strings.HasPrefix(lowerHref, "javascript") {

			aUrl, err := url.Parse(href)
			if err != nil {
				errs = append(errs, err)
				return
			}

			if !aUrl.IsAbs() {
				aUrl = httpResp.Request.URL.ResolveReference(aUrl)
			}

			httpReq, err := http.NewRequest("GET", aUrl.String(), nil)

			if err != nil {
				errs = append(errs, err)
			} else {
				req := base.NewRequest(httpReq, respDepth)
				dataList = append(dataList, req)
			}
		}
		text := strings.TrimSpace(sel.Text())
		if text != "" {
			imap := make(map[string]interface{})
			imap["a.text"] = text
			imap["parent_url"] = httpResp.Request.URL
			item := base.Item{}
			dataList = append(dataList, &item)

		}

	})

	return dataList, errs

}

func processItem(item base.Item) (result base.Item, err error) {
	if item == nil {
		return nil, errors.New("Invalid item!")
	}

	result = make(map[string]interface{})

	for k, v := range item {
		result[k] = v
	}

	if _, ok := result["number"]; !ok {
		result["number"] = len(result)
	}

	time.Sleep(10 * time.Millisecond)

}
