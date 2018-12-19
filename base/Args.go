package base

import (
	"errors"
	"fmt"
)

type Args interface {
	Check() error
	String() string
}

type ChannelArgs struct {
	reqChanLen   uint
	respChanLen  uint
	itemChanLen  uint
	errorChanLen uint
	description  string
}

func NewChannelArgs(reqChanLen uint, respChanLen uint, itemChanLen uint, errorChanLen uint) ChannelArgs {
	return ChannelArgs{reqChanLen: reqChanLen, respChanLen: respChanLen, itemChanLen: itemChanLen, errorChanLen: errorChanLen}
}

func (args *ChannelArgs) ReqChanLen() uint {
	return args.reqChanLen
}

func (args *ChannelArgs) RespChanLen() uint {
	return args.respChanLen
}

func (args *ChannelArgs) ItemChanLen() uint {
	return args.itemChanLen
}

func (args *ChannelArgs) ErrorChanLen() uint {
	return args.errorChanLen
}

func (args *ChannelArgs) Check() error {

	if args.respChanLen <= 0 {
		return errors.New(fmt.Sprintf("the response channel length is invalid.(value=%d)\n", args.respChanLen))
	}

	if args.reqChanLen <= 0 {
		return errors.New(fmt.Sprintf("the request channel length is invalid.(value=%d)\n", args.reqChanLen))
	}

	if args.itemChanLen <= 0 {
		return errors.New(fmt.Sprintf("the item channel length is invalid.(value=%d)\n", args.itemChanLen))
	}

	if args.errorChanLen <= 0 {
		return errors.New(fmt.Sprintf("the error channel length is invalid.(value=%d)\n", args.errorChanLen))
	}

	return nil
}

var channelArgsTemplate = "Response Channel Length:%d\n, Request Channel Length:%d\n, Item Channel Length:%d\n, Error Channel Length:%d\n"

func (args *ChannelArgs) String() string {

	if args.description == "" {
		args.description = fmt.Sprintf(channelArgsTemplate, args.respChanLen, args.reqChanLen, args.itemChanLen, args.errorChanLen)
	}
	return args.description
}

type PoolBaseArgs struct {
	downloadPoolSize uint32
	analyzerPoolSize uint32
	description      string
}

func NewPoolBaseArgs(downloadPoolSize uint32, analyzerPoolSize uint32) PoolBaseArgs {
	return PoolBaseArgs{downloadPoolSize: downloadPoolSize, analyzerPoolSize: analyzerPoolSize}
}

func (args *PoolBaseArgs) Check() error {
	if args.downloadPoolSize <= 0 {
		return errors.New(fmt.Sprintf("the Downloader Pool Size is invalid.(value=%d)", args.downloadPoolSize))
	}

	if args.analyzerPoolSize <= 0 {
		return errors.New(fmt.Sprintf("the Analyzer Pool Size is invalid.(value=%d)", args.analyzerPoolSize))
	}
	return nil
}

var poolArgsTemplate = " Downloader Pool Size:%d \n Analyzer Pool Size:%d\n"

func (args *PoolBaseArgs) String() string {
	if args.description == "" {
		args.description = fmt.Sprintf(poolArgsTemplate, args.downloadPoolSize, args.analyzerPoolSize)
	}

	return args.description
}

func (args *PoolBaseArgs) DownloaderPoolSize() uint32 {
	return args.downloadPoolSize
}

func (args *PoolBaseArgs) AnalyzerPoolSize() uint32 {
	return args.analyzerPoolSize
}
