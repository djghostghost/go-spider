package middleware

import (
	"../base"
	"errors"
	"fmt"
	"sync"
)

type ChannelManager interface {
	Init(channelLen uint, reset bool) bool
	Close() bool
	ReqChan() (chan base.Request, error)
	RespChan() (chan base.Response, error)
	ItemChan() (chan base.Item, error)
	ErrorChan() (chan error, error)
	ChannelLen() uint
	Status() ChannelManagerStatus
	Summary() string
}

type ChannelManagerStatus uint

var channelSummaryTemplate = "status:%s, requestChannel:%d%d, responseChannel:%d%d, itemChannel:%d%d, errorChannel:%d%d "

const default_chan_len = 256

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
	channelLen uint
	reqCh      chan base.Request
	respCh     chan base.Response
	itemCh     chan base.Item
	errorCh    chan error
	status     ChannelManagerStatus
	rwMutex    sync.RWMutex
}

func NewChannelManager(channelLen uint) ChannelManager {

	if channelLen == 0 {
		channelLen = default_chan_len
	}

	chanman := &ChannelManagerInfo{}
	chanman.Init(channelLen, true)
	return chanman
}

func (channel *ChannelManagerInfo) Init(channelLen uint, reset bool) bool {

	if channelLen == 0 {
		panic(errors.New("the channel length is invalid"))
	}

	channel.rwMutex.Lock()
	defer channel.rwMutex.Unlock()
	//防止重复初始化
	if channel.status == CHANNEL_MANAGER_STATUS_INITIALIZED && !reset {
		return false
	}

	channel.channelLen = channelLen
	channel.reqCh = make(chan base.Request, channelLen)
	channel.respCh = make(chan base.Response, channelLen)
	channel.errorCh = make(chan error, channelLen)
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

func (channel *ChannelManagerInfo) ChannelLen() uint {
	return channel.channelLen
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
