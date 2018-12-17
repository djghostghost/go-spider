package base

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type Entity interface {
	Id() uint32
}

type Pool interface {
	Take() (Entity, error)
	Return(entity Entity) error
	Total() uint32
	Used() uint32
}

type poolImpl struct {
	total       uint32
	eType       reflect.Type
	genEntity   func() Entity
	container   chan Entity
	idContainer map[uint32]bool
	mutex       sync.Mutex
}

func NewPool(total uint32, entityType reflect.Type, genEntity func() Entity) (Pool, error) {

	if total == 0 {
		errMsg := fmt.Sprintf("The pool can not be initialized!(total=%d)\n", total)
		return nil, errors.New(errMsg)
	}

	size := int(total)

	idContainer := make(map[uint32]bool, size)
	container := make(chan Entity, size)

	for i := 0; i < size; i++ {
		newEntity := genEntity()

		if entityType != reflect.TypeOf(newEntity) {
			errMsg := fmt.Sprintf("The type of result of function genEntity() is NOT %s\n", entityType)
			return nil, errors.New(errMsg)
		}
		idContainer[newEntity.Id()] = true
		container <- newEntity
	}

	pool := &poolImpl{total: total, eType: entityType, genEntity: genEntity, container: container, idContainer: idContainer}
	return pool, nil
}

func (pool *poolImpl) Take() (Entity, error) {
	entity, ok := <-pool.container

	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	pool.idContainer[entity.Id()] = false
	if !ok  {
		return nil, errors.New("the pool container was closed")
	}
	return entity, nil
}

func (pool *poolImpl) Return(entity Entity) error {
	if entity == nil {
		return errors.New("the type of returning entity is nil")
	}

	if pool.eType != reflect.TypeOf(entity) {
		return errors.New(fmt.Sprintf("the type of returning is invalid:%s", reflect.TypeOf(entity)))
	}
	result := pool.compareAndSetForIdContainer(entity.Id(), false, true)

	if result == 0 {
		errMsg := fmt.Sprintf("the entity (id=%s) is illegal!", entity.Id())
		return errors.New(errMsg)
	} else if result == -1 {
		errMsg := fmt.Sprintf("the entity (id=%d) is illegal", entity.Id())
		return errors.New(errMsg)
	} else {
		pool.container <- entity
		return nil
	}
}

func (pool *poolImpl) compareAndSetForIdContainer(entityId uint32,
	oldValue bool, newValue bool) int8 {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	v, ok := pool.idContainer[entityId]

	if !ok {
		return -1
	}

	if v != oldValue {
		return 0
	}

	pool.idContainer[entityId] = newValue
	return 1
}

func (pool *poolImpl) Total() uint32 {
	return pool.total
}

func (pool *poolImpl) Used() uint32 {
	return pool.total - uint32(len(pool.container))
}
