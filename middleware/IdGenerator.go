package middleware

import (
	"math"
	"sync"
)

type IdGenerator interface {
	GetUint32() uint32
}

type cycleGenerator struct {
	sn    uint32
	ended bool
	mutex sync.Mutex
}

func NewIdGenerator() IdGenerator {
	return &cycleGenerator{}
}

func (generator *cycleGenerator) GetUint32() uint32 {

	generator.mutex.Lock()
	defer generator.mutex.Unlock()

	if generator.ended {
		defer func() { generator.ended = false }()
		generator.sn = 0
		return generator.sn
	}

	id := generator.sn
	if id < math.MaxUint32 {
		generator.sn++
	} else {
		generator.ended = true
	}
	return id
}

type cycleGenerator2 struct {
	base       cycleGenerator
	cycleCount uint64
}

func (generator *cycleGenerator2) GetUint64() uint64 {

	var id64 uint64

	if generator.cycleCount%2 == 1 {
		id64 += math.MaxInt32
	}

	id32 := generator.base.GetUint32()

	if id32 == math.MaxInt32 {
		generator.cycleCount++
	}

	id64 += uint64(id32)
	return id64
}
