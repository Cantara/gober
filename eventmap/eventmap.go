package eventmap

import (
	"context"
	"fmt"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/syncmap"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/stream/event/store"
)

type EventMap[DT any] interface {
	Get(key string) (data DT, err error)
	Exists(key string) (exists bool)
	Len() (l int)
	Keys() (keys []string)
	Range(f func(key string, value DT) bool)
	GetMap() map[string]DT
	Delete(key string) (err error)
	Set(key string, data DT) (err error)
	Stream(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out <-chan event.Event[DT], err error)
}

type mapData[DT any] struct {
	data             syncmap.SyncMap[DT]
	eventTypeName    string
	eventTypeVersion string
	provider         stream.CryptoKeyProvider
	es               consumer.Consumer[kv[DT]]
}

type kv[DT any] struct {
	Key   string `json:"key"`
	Value DT     `json:"value"`
}

func Init[DT any](pers stream.Stream, eventType, dataTypeVersion string, p stream.CryptoKeyProvider, ctx context.Context) (ed EventMap[DT], err error) {
	es, err := consumer.New[kv[DT]](pers, p, ctx)
	m := mapData[DT]{
		data:             syncmap.New[DT](),
		eventTypeName:    eventType,
		eventTypeVersion: dataTypeVersion,
		provider:         p,
		es:               es,
	}
	if err != nil {
		return
	}
	readChan, err := es.Stream(event.AllTypes(), store.STREAM_START, stream.ReadAll(), ctx)
	if err != nil {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-readChan:
				func() {
					defer e.Acc()
					if e.Type == event.Deleted {
						m.data.Delete(e.Data.Key)
						return
					}
					m.data.Set(e.Data.Key, e.Data.Value)
				}()
			}
		}
	}()

	ed = &m
	return
}

func (m *mapData[DT]) Stream(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out <-chan event.Event[DT], err error) {
	s, err := m.es.Stream(eventTypes, from, filter, ctx)
	if err != nil {
		return
	}
	c := make(chan event.Event[DT])
	go func() {
		for e := range s {
			select {
			case <-ctx.Done():
				return
			case c <- event.Event[DT]{
				Type:     e.Type,
				Data:     e.Event.Data.Value,
				Metadata: e.Metadata,
			}:
				continue
			}
		}
	}()
	out = c
	return
}

var ERROR_KEY_NOT_FOUND = fmt.Errorf("provided key does not exist")

func (m *mapData[DT]) Get(key string) (data DT, err error) {
	ed, ok := m.data.Get(key)
	if !ok {
		err = ERROR_KEY_NOT_FOUND
		return
	}
	return ed, nil
}

func (m *mapData[DT]) Len() (l int) {
	return len(m.Keys())
}

func (m *mapData[DT]) Keys() (keys []string) {
	keys = make([]string, 0)
	m.Range(func(k string, _ DT) bool {
		keys = append(keys, k)
		return true
	})
	return
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot of the Map's
// contents: no key will be visited more than once, but if the value for any key
// is stored or deleted concurrently (including by f), Range may reflect any
// mapping for that key from any point during the Range call. Range does not
// block other methods on the receiver; even f itself may call any method on m.
func (m *mapData[DT]) Range(f func(key string, value DT) bool) {
	for k, v := range m.GetMap() {
		if !f(k, v) {
			return
		}
	}
}

func (m *mapData[DT]) GetMap() map[string]DT {
	return m.data.GetMap()
}

func (m *mapData[DT]) Exists(key string) (exists bool) {
	_, exists = m.data.Get(key)
	return
}

func (m *mapData[DT]) createEvent(key string, data DT) (e event.Event[kv[DT]], err error) {
	eventType := event.Created
	if m.Exists(key) {
		eventType = event.Updated
	}

	e = event.Event[kv[DT]]{
		Type: eventType,
		Data: kv[DT]{
			Key:   key,
			Value: data,
		},
		Metadata: event.Metadata{
			Version:  m.eventTypeVersion,
			DataType: m.eventTypeName,
			Key:      crypto.SimpleHash(key),
		},
	}
	return
}

func (m *mapData[DT]) Delete(key string) (err error) {
	e := event.Event[kv[DT]]{
		Type: event.Deleted,
		Data: kv[DT]{
			Key: key,
		},
		Metadata: event.Metadata{
			Version:  m.eventTypeVersion,
			DataType: m.eventTypeName,
			Key:      crypto.SimpleHash(key),
		},
	}
	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() //Missing error
	return
}

func (m *mapData[DT]) Set(key string, data DT) (err error) {
	log.Trace("Set and wait start", "key", key)
	e, err := m.createEvent(key, data)
	if err != nil {
		return
	}
	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() //Missing error
	log.Trace("Set and wait end", "key", key)
	return
}
