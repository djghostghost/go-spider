package middleware

import (
	"../base"
	"errors"
	"fmt"
	"sync"
)

type ChannelManager interface {
	Init(channelArgs base.ChannelArgs, reset bool) bool
	Close() bool
	ReqChan() (chan base.Request, error)
	RespChan() (chan base.Response, error)
	ItemChan() (chan base.Item, error)
	ErrorChan() (chan error, error)
	ChannelArgs() base.ChannelArgs
	Status() ChannelManagerStatus
	Summary() string
}

type ChannelManagerStatus uint

var channelSummaryTemplate = "status:%s, requestChannel:%d%d, responseChannel:%d%d, itemChannel:%d%d, errorChannel:%d%d "

const (
	CHANNEL_MANAGER_STATUS_UNINITIALIZED ChannelManagerStatus = 0
	CHANNEL_MANAGER_STATUS_INITIALIZED   ChannelManagerStatus = 1
	CHANNEL_MANAGER_STATUS_CLOSED        ChannelManagerStatus = 2
)

var statusNameMap = map[ChannelManagerStatus]string{

	CHANNEL_MANAGER_STATUS_UNINITIALIZED: "uninitialized",
	CHANNEL_MANAGER_STATUS_INITIALIZED:   "initialized",
	CHANNEL_MANAGER_STATUS_CLOSED:        "closed",
}

type ChannelManagerInfo struct {
	channelArgs base.ChannelArgs
	reqCh       chan base.Request
	respCh      chan base.Response
	itemCh      chan base.Item
	errorCh     chan error
	status      ChannelManagerStatus
	rwMutex     sync.RWMutex
}

func NewChannelManager(channelArgs base.ChannelArgs) ChannelManager {

	chanman := &ChannelManagerInfo{}
	chanman.Init(channelArgs, true)
	return chanman
}

func (channel *ChannelManagerInfo) Init(channelArgs base.ChannelArgs, reset bool) bool {

	if err := channelArgs.Check(); err != nil {
		panic(err)
	}

	channel.rwMutex.Lock()
	defer channel.rwMutex.Unlock()
	//防止重复初始化
	if channel.status == CHANNEL_MANAGER_STATUS_INITIALIZED && !reset {
		return false
	}

	channel.channelArgs = channelArgs
	channel.reqCh = make(chan base.Request, channelArgs.ReqChanLen())
	channel.respCh = make(chan base.Response, channelArgs.RespChanLen())
	channel.errorCh = make(chan error, channelArgs.ErrorChanLen())
	channel.itemCh = make(chan base.Item, channelArgs.ItemChanLen())
	channel.status = CHANNEL_MANAGER_STATUS_INITIALIZED
	return true
}

func (channel *ChannelManagerInfo) Close() bool {
	channel.rwMutex.Lock()
	defer channel.rwMutex.Unlock()

	if channel.status != CHANNEL_MANAGER_STATUS_INITIALIZED {
		return false
	}

	close(channel.reqCh)
	close(channel.respCh)
	close(channel.itemCh)
	close(channel.errorCh)
	channel.status = CHANNEL_MANAGER_STATUS_CLOSED
	return true
}

func (channel *ChannelManagerInfo) ReqChan() (chan base.Request, error) {
	channel.rwMutex.RLock()
	defer channel.rwMutex.RUnlock()

	if err := channel.checkStatus(); err != nil {
		return nil, err
	}
	return channel.reqCh, nil
}

func (channel *ChannelManagerInfo) RespChan() (chan base.Response, error) {

	channel.rwMutex.RLock()
	defer channel.rwMutex.RUnlock()
	if err := channel.checkStatus(); err != nil {
		return nil, err
	}
	return channel.respCh, nil
}

func (channel *ChannelManagerInfo) ItemChan() (chan base.Item, error) {
	channel.rwMutex.RLock()
	defer channel.rwMutex.RUnlock()
	if err := channel.checkStatus(); err != nil {
		return nil, err
	}
	return channel.itemCh, nil
}

func (channel *ChannelManagerInfo) ErrorChan() (chan error, error) {
	channel.rwMutex.RLock()
	defer channel.rwMutex.RUnlock()
	if err := channel.checkStatus(); err != nil {
		return nil, err
	}
	return channel.errorCh, nil
}

func (channel *ChannelManagerInfo) ChannelArgs() base.ChannelArgs {
	return channel.channelArgs
}

func (channel *ChannelManagerInfo) Status() ChannelManagerStatus {
	return channel.status
}

func (channel *ChannelManagerInfo) Summary() string {
	summary := fmt.Sprintf(channelSummaryTemplate, statusNameMap[channel.status],
		len(channel.reqCh), cap(channel.reqCh),
		len(channel.respCh), cap(channel.respCh),
		len(channel.itemCh), cap(channel.itemCh),
		len(channel.errorCh), cap(channel.errorCh))
	return summary
}

func (channel *ChannelManagerInfo) checkStatus() error {
	if channel.status == CHANNEL_MANAGER_STATUS_INITIALIZED {
		return nil
	}

	statusName, ok := statusNameMap[channel.status]

	if !ok {
		statusName = fmt.Sprintf("%d", channel.status)
	}

	errMsg := fmt.Sprintf("The undersirable status of channel manager:%s\n", statusName)
	return errors.New(errMsg)
}
