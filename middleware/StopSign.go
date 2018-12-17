package middleware

import (
	"fmt"
	"sync"
)

type StopSign interface {
	Sign() bool
	//判断新后是否已发出
	Signed() bool

	Reset()
	//代表停止信号处理的方式
	Deal(code string)

	DealCount(code string) uint32

	DealTotal() uint32

	Summary() string
}

type stopSignImpl struct {
	signed       bool
	dealCountMap map[string]uint32
	rwMutex      sync.Mutex
}

func NewStopSign() StopSign {
	ss := &stopSignImpl{dealCountMap: make(map[string]uint32)}

	return ss
}

func (sign *stopSignImpl) Sign() bool {
	sign.rwMutex.Lock()

	defer sign.rwMutex.Unlock()

	if sign.signed {
		return false
	}
	sign.signed = true
	return true
}

func (sign *stopSignImpl) Signed() bool {
	return sign.signed
}

func (sign *stopSignImpl) Deal(code string) {

	sign.rwMutex.Lock()
	defer sign.rwMutex.Unlock()

	if !sign.signed {
		return
	}

	if _, ok := sign.dealCountMap[code]; !ok {
		sign.dealCountMap[code] = 1
	} else {
		sign.dealCountMap[code] += 1
	}

}

func (sign *stopSignImpl) Reset() {
	sign.rwMutex.Lock()
	defer sign.rwMutex.Unlock()
	sign.signed = false
	sign.dealCountMap = make(map[string]uint32)
}

func (sign *stopSignImpl) DealCount(code string) uint32 {
	sign.rwMutex.Lock()
	defer sign.rwMutex.Unlock()
	if v, ok := sign.dealCountMap[code]; ok {
		return v
	} else{
		return 0
	}
}

func (sign *stopSignImpl) DealTotal() uint32 {

	sign.rwMutex.Lock()
	defer sign.rwMutex.Unlock()
	sum := uint32(0)
	for _, v := range sign.dealCountMap {
		sum += v
	}
	return sum
}

func (sign *stopSignImpl) Summary() string{
	return fmt.Sprintf("Status:%s, DealCount: %d",sign.signed,sign.DealTotal())
}
