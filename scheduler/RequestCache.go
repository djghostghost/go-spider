package scheduler

import (
	"../base"
	"fmt"
	"sync"
)

type requestCache interface {
	put(req *base.Request) bool
	get() *base.Request
	capacity() int
	length() int
	close()
	summary() string
}

type requestCacheSlice struct {
	cache  []*base.Request
	mutex  sync.Mutex
	status byte
}

func newRequestCache() requestCache {
	return &requestCacheSlice{cache: make([]*base.Request, 0)}
}

func (cache *requestCacheSlice) put(req *base.Request) bool {

	if req == nil {
		return false
	}

	if cache.status == 1 {
		return false
	}
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	cache.cache = append(cache.cache, req)
	return true
}

func (cache *requestCacheSlice) get() *base.Request {
	if cache.length() == 0 {
		return nil
	}

	if cache.status == 1 {
		return nil
	}

	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	req := cache.cache[0]
	cache.cache = cache.cache[1:]
	return req
}

func (cache *requestCacheSlice) length() int {
	return len(cache.cache)
}

func (cache *requestCacheSlice) capacity() int {
	return cap(cache.cache)
}

func (cache *requestCacheSlice) close() {
	if cache.status == 1 {
		return
	}
	cache.status = 1
}

var statusMap = map[byte]string{
	0: "running",
	1: "stop",
}

func (cache *requestCacheSlice) summary() string {
	template := "status:%s, length:%d, capacity:%d"
	summary := fmt.Sprintf(template, statusMap[cache.status], cache.length(), cache.capacity())
	return summary
}
