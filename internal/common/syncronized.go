package common

import (
	"github.com/spaolacci/murmur3"
	"sync"
)

type Syncronized struct {
	LockNum   uint64
	resultMap sync.Map
}

func NewSyncronized(lockNum uint64) Syncronized {
	if lockNum == 0 {
		lockNum = 64
	}
	syncronized := Syncronized{
		LockNum: lockNum,
	}

	return syncronized
}

func (s *Syncronized) getLock(key string) *sync.Mutex {
	code := hashCode(key)
	index := code % s.LockNum
	mutex, _ := s.resultMap.LoadOrStore(index, new(sync.Mutex))
	lock := mutex.(*sync.Mutex)
	return lock
}

func (s *Syncronized) Lock(key string) {
	lock := s.getLock(key)
	lock.Lock()
}

//func (s *Syncronized) TryLock(key string) {
//	lock := s.getLock(key)
//	lock.TryLock()
//}

func (s *Syncronized) Unlock(key string) {
	lock := s.getLock(key)
	lock.Unlock()
}

func hashCode(data string) uint64 {
	return murmur3.Sum64([]byte(data))
}
