package analyz

import (
	"../base"
	"errors"
	"fmt"
	"reflect"
)
type AnalyzerPool interface {
	Take() (Analyzer, error)
	Return(Analyzer Analyzer) error
	Total() uint32
	Used() uint32
}

type goAnalyzerPool struct{
	pool base.Pool
	eType reflect.Type
}

type GenAnalyzer func() Analyzer

func NewAnalyzerPool(total uint32, gen GenAnalyzer) (AnalyzerPool,error){

	genEntity := func() base.Entity{
		return gen()
	}

	eType := reflect.TypeOf(gen)

	pool,err:= base.NewPool(total,eType,genEntity)
	if err!=nil{
		return nil,err
	}

	return &goAnalyzerPool{pool:pool,eType:eType},nil
}

func (pool *goAnalyzerPool) Take()(Analyzer,error){
	entity,err := pool.pool.Take()
	if err!=nil{
		return nil,err
	}

	analyzerEntity, ok := entity.(Analyzer)

	if !ok{
		errMsg := fmt.Sprintf("the type of entity is NOT %s", pool.eType)
		panic(errors.New(errMsg))
	}
	return analyzerEntity,nil
}

func (pool *goAnalyzerPool) Return(entity Analyzer) error{
	return pool.pool.Return(entity)
}

func (pool *goAnalyzerPool) Total() uint32{
	return pool.pool.Total()
}

func (pool *goAnalyzerPool) Used() uint32{
	return pool.pool.Used()
}
