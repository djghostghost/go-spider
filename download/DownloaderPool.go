package download

import (
	"../base"
	"errors"
	"fmt"
	"reflect"
)

type goDownloaderPool struct {
	pool  base.Pool
	eType reflect.Type
}

//downloader pool
type PageDownloaderPool interface {
	Take() (PageDownloader, error)
	Return(PageDownloader) error
	Total() uint32
	Used() uint32
}

type GenPageDownloader func() PageDownloader

func NewPageDownloaderPool(total uint32, gen GenPageDownloader) (PageDownloaderPool, error) {

	genEntity := func() base.Entity {
		return gen()
	}
	eType := reflect.TypeOf(gen)
	pool, err := base.NewPool(total, eType, genEntity)
	if err != nil {
		return nil, err
	}
	return &goDownloaderPool{pool: pool, eType: eType},nil
}

func (downloader *goDownloaderPool) Take() (PageDownloader, error) {
	entity, err := downloader.pool.Take()

	if err != nil {
		return nil, err
	}

	downloaderEntity, ok := entity.(PageDownloader)

	if !ok {
		errMsg := fmt.Sprintf("The type of entity is NOT %s", downloader.eType)
		panic(errors.New(errMsg))
	}
	return downloaderEntity, nil
}

func (downloader *goDownloaderPool) Return(entity PageDownloader) error{
	return downloader.pool.Return(entity)
}

func (downloader *goDownloaderPool) Total() uint32{
	return downloader.pool.Total()
}

func (downloader *goDownloaderPool) Used() uint32{
	return downloader.pool.Used()
}
