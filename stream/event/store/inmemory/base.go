package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/cantara/gober/stream/event/store"
)

type inMemEvent struct {
	Event    store.Event
	Position uint64
	Created  time.Time
}

// stream Need to add a way to not store multiple events with the same id in the same stream.
type stream struct {
	db       []inMemEvent
	dbLock   *sync.Mutex
	newData  *sync.Cond
	position uint64
}

type Stream struct {
	data      stream
	name      string
	writeChan chan<- store.WriteEvent
	ctx       context.Context
}

func Init(name string, ctx context.Context) (es *Stream, err error) {
	writeChan := make(chan store.WriteEvent, 0)
	es = &Stream{
		data: stream{
			db:      make([]inMemEvent, 0),
			dbLock:  &sync.Mutex{},
			newData: sync.NewCond(&sync.Mutex{}),
		},
		name:      name,
		writeChan: writeChan,
		ctx:       ctx,
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-writeChan:
				func() {
					es.data.dbLock.Lock()
					defer es.data.dbLock.Unlock()
					defer func() {
						if e.Status != nil {
							close(e.Status)
						}
					}()
					se := inMemEvent{
						Event:    e.Event,
						Position: uint64(len(es.data.db) + 1),
						Created:  time.Now(),
					}

					es.data.db = append(es.data.db, se)
					es.data.position = se.Position
					if e.Status != nil {
						e.Status <- store.WriteStatus{
							Time:     se.Created,
							Position: se.Position,
						}
					}

					es.data.newData.Broadcast()
				}()
			}
		}
	}()
	return
}

func (es *Stream) Write() chan<- store.WriteEvent {
	return es.writeChan
}

func (es *Stream) Stream(from store.StreamPosition, ctx context.Context) (out <-chan store.ReadEvent, err error) {
	eventChan := make(chan store.ReadEvent, 2)
	out = eventChan
	go func() {
		defer close(eventChan)
		for {
			select {
			case <-ctx.Done():
				return
			case <-es.ctx.Done():
				return
			default:
			}
			position := uint64(from)
			if from == store.STREAM_END {
				position = uint64(len(es.data.db))
			}
			for {
				select {
				case <-ctx.Done():
					return
				case <-es.ctx.Done():
					return
				default:
					for ; position < uint64(len(es.data.db)); position++ {
						se := es.data.db[position]
						eventChan <- store.ReadEvent{
							Event:    se.Event,
							Position: se.Position,
							Created:  se.Created,
						}
					}
					if position >= uint64(len(es.data.db)) {
						es.data.newData.L.Lock()
						es.data.newData.Wait()
						es.data.newData.L.Unlock()
					}
				}
			}
		}
	}()
	return
}

func (es *Stream) Name() string {
	return es.name
}

func (es *Stream) End() (pos uint64, err error) {
	pos = es.data.position
	return
}
