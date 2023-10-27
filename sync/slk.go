package sync

import (
	"sync"
	"time"
)

type SLK interface {
	Add(key string, timeout time.Duration)
	Get(key string) (ok bool)
	Delete(key string)
}

func NewSLK() SLK {
	return &slk{
		rwLock: sync.RWMutex{},
		data:   make(map[string]time.Time),
	}
}

type slk struct {
	rwLock sync.RWMutex
	data   map[string]time.Time
}

func (s *slk) Add(key string, timeout time.Duration) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data[key] = time.Now().Add(timeout)
}

func (s *slk) Get(key string) (ok bool) {
	s.rwLock.RLock()
	timeout, ok := s.data[key]
	s.rwLock.RUnlock()
	if !ok {
		return
	}
	go s.Delete(key) //TODO: change it to use a watcher thread instead of this simple hack. This cuold leak mem by not cleaning old values that is not getting accessed
	return time.Now().After(timeout)
}

func (s *slk) Delete(key string) {
	s.rwLock.RLock()
	_, ok := s.data[key]
	s.rwLock.RUnlock()
	if !ok {
		return
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	delete(s.data, key)
	return
}
