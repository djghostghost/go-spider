package pipeline

import (
	"../base"
	"errors"
	"fmt"
	"sync/atomic"
)

type ItemPipeline interface {
	Send(item base.Item) []error
	FailFast() bool
	SetFailFast(failFast bool)
	//return send receive analyzed data
	Count() []uint64
	ProcessingNumber() uint64
	Summary() string
}

type ProcessItem func(item base.Item) (result base.Item, err error)

type goItemPipeLine struct {
	itemProcessors   []ProcessItem
	failFast         bool
	sent             uint64
	accepted         uint64
	processed        uint64
	processingNumber uint64
}

func (pipeLine *goItemPipeLine) SetFailFast(failFast bool) {
	pipeLine.failFast = failFast
}

func (pipeLine *goItemPipeLine) ProcessingNumber() uint64 {
	return atomic.LoadUint64(&pipeLine.processingNumber)
}

func (pipeLine *goItemPipeLine) FailFast() bool {
	return pipeLine.failFast
}

func (pipeLine *goItemPipeLine) Send(item base.Item) []error {

	atomic.AddUint64(&pipeLine.processingNumber, 1)
	defer atomic.AddUint64(&pipeLine.processingNumber, ^uint64(0))
	atomic.AddUint64(&pipeLine.sent, 1)
	errs := make([]error, 0)
	if item == nil {
		errs = append(errs, errors.New("the item is invalid"))
		return errs
	}

	atomic.AddUint64(&pipeLine.accepted, 1)
	currentItem := item
	for _, itemProcessor := range pipeLine.itemProcessors {

		processedItem, err := itemProcessor(currentItem)
		if err != nil {
			errs = append(errs, err)
			if pipeLine.failFast {
				break
			}
		}
		if processedItem != nil {
			currentItem = processedItem
		}
	}
	atomic.AddUint64(&pipeLine.processed, 1)
	return nil
}

func (pipeLine *goItemPipeLine) Count() []uint64 {

	counts := make([]uint64, 3)
	counts[0] = atomic.LoadUint64(&pipeLine.sent)
	counts[1] = atomic.LoadUint64(&pipeLine.accepted)
	counts[2] = atomic.LoadUint64(&pipeLine.processed)
	return counts
}

var summaryTemplate = "failFast:%v, processorNumber:%d, sent:%d, accepted:%d, processed:%d, processingNumber:%d\n"

func (pipeLine *goItemPipeLine) Summary() string {
	counts := pipeLine.Count()
	summary := fmt.Sprintf(summaryTemplate, pipeLine.failFast, len(pipeLine.itemProcessors), counts[0], counts[1], counts[2], pipeLine.ProcessingNumber())
	return summary
}

func NewItemPipeLine(itemProcessors []ProcessItem) ItemPipeline {

	if itemProcessors == nil {
		panic(errors.New(fmt.Sprintln("Invalid item processor list!")))
	}

	innerItemProcessors := make([]ProcessItem, 0)

	for i, ip := range itemProcessors {
		if ip == nil {
			panic(errors.New(fmt.Sprintf("invalid item processor [%d]\n", i)))
		}
		innerItemProcessors = append(innerItemProcessors, ip)
	}

	return &goItemPipeLine{itemProcessors: innerItemProcessors}
}
