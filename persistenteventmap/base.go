package persistenteventmap

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"

	"github.com/cantara/gober/stream/consumer"
	"github.com/dgraph-io/badger/options"

	"github.com/dgraph-io/badger"

	log "github.com/cantara/bragi/sbragi"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
)

type EventMap[DT any] interface {
	Get(key string) (data DT, err error)
	Exists(key string) (exists bool)
	Len() (l int)
	Keys() (keys []string)
	Range(f func(key string, data DT) error)
	Delete(data DT) (err error)
	Set(data DT) (err error)
	Stream(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out <-chan event.Event[DT], err error)
}

type transactionCheck struct {
}

type mapData[DT any] struct {
	data            *badger.DB
	transactionChan chan transactionCheck
	dataTypeName    string
	dataTypeVersion string
	provider        stream.CryptoKeyProvider
	es              consumer.Consumer[DT]
	ctx             context.Context
	getKey          func(dt DT) string
}

func Init[DT any](s stream.Stream, dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, getKey func(dt DT) string, ctx context.Context) (ed EventMap[DT], err error) {
	path := "./eventmap/" + dataTypeName
	os.MkdirAll(path, 0750)
	db, err := badger.Open(badger.DefaultOptions(path).
		WithMaxTableSize(1024 * 1024 * 8).
		WithValueLogFileSize(1024 * 1024 * 8).
		WithValueLogLoadingMode(options.FileIO))
	if err != nil {
		return
	}
	from := store.STREAM_START
	positionKey := []byte(fmt.Sprintf("%s_%s_position", s.Name(), dataTypeName))
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(positionKey)
		if err != nil {
			return err
		}
		var pos uint64
		err = item.Value(func(val []byte) error {
			pos = binary.LittleEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return err
		}
		from = store.StreamPosition(pos)
		return nil
	})
	es, err := consumer.New[DT](s, p, ctx)
	if err != nil {
		return
	}
	m := mapData[DT]{
		data:            db,
		transactionChan: make(chan transactionCheck),
		dataTypeName:    dataTypeName,
		dataTypeVersion: dataTypeVersion,
		provider:        p,
		es:              es,
		getKey:          getKey,
	}
	eventChan, err := es.Stream(event.AllTypes(), from, stream.ReadDataType(dataTypeName), ctx)
	if err != nil {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-eventChan:
				if e.Type == event.Delete {
					err := db.Update(func(txn *badger.Txn) error {
						err = txn.Delete([]byte(getKey(e.Data)))
						if err != nil {
							return err
						}

						pos := make([]byte, 8)
						binary.LittleEndian.PutUint64(pos, e.Position)
						return txn.Set(positionKey, pos)
					})
					if err != nil {
						log.WithError(err).Error("Delete error")
						continue
					}
					e.Acc()
				}
				data, err := json.Marshal(e.Data)
				if err != nil {
					return
				}
				err = db.Update(func(txn *badger.Txn) error {
					err = txn.Set([]byte(getKey(e.Data)), data)
					if err != nil {
						return err
					}

					pos := make([]byte, 8)
					binary.LittleEndian.PutUint64(pos, e.Position)
					return txn.Set(positionKey, pos)
				})
				if err != nil {
					log.WithError(err).Error("Update error")
					continue
				}
				e.Acc()
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
			case c <- e.Event:
				continue
			}
		}
	}()
	out = c
	return
}

var ERROR_KEY_NOT_FOUND = fmt.Errorf("provided key does not exist")

func (m *mapData[DT]) Get(key string) (data DT, err error) {
	var ed []byte
	err = m.data.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		ed, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return
	}

	err = json.Unmarshal(ed, &data)
	if err != nil {
		return
	}
	return
}

func (m *mapData[DT]) Len() (l int) {
	return len(m.Keys())
}

func (m *mapData[DT]) Keys() (keys []string) {
	keys = make([]string, 0)
	m.data.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			keys = append(keys, string(k))
		}
		return nil
	})
	return
}

func (m *mapData[DT]) Range(f func(key string, data DT) error) {
	m.data.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				var data DT
				err := json.Unmarshal(v, &data)
				if err != nil {
					return err
				}
				return f(string(k), data)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *mapData[DT]) Exists(key string) (exists bool) {
	_, err := m.Get(key)
	return err == nil
}

func (m *mapData[DT]) createEvent(data DT) (e event.Event[DT], err error) {
	key := m.getKey(data)
	eventType := event.Create
	if m.Exists(key) {
		eventType = event.Update
	}

	e = event.Event[DT]{
		Type: eventType,
		Data: data,
		Metadata: event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(key),
		},
	}
	return
}

func (m *mapData[DT]) Delete(data DT) (err error) {
	e := event.Event[DT]{
		Type: event.Delete,
		Data: data,
		Metadata: event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(m.getKey(data)),
		},
	}

	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() //Missing error
	return
}

func (m *mapData[DT]) Set(data DT) (err error) {
	log.Trace("Set and wait start")
	e, err := m.createEvent(data)
	if err != nil {
		return
	}
	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() //Missing error
	log.Trace("Set and wait end")
	return
}
