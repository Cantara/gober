package persistentbigmap

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/webserver"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/gin-gonic/gin"
	"github.com/gofrs/uuid"
	"io"
	"net/http"
	"strings"

	log "github.com/cantara/bragi/sbragi"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
)

type EventMap[DT, MT any] interface {
	Get(key string) (data DT, err error)
	Len() (l int)
	Keys() (keys []string)
	Range(f func(key string, data DT) error)
	Delete(data MT) (err error)
	Set(data DT, meta MT) (err error)
	Exists(key string) (exists bool)
}

type transactionCheck struct {
}

type mapData[DT, MT any] struct {
	data              *badger.DB
	transactionChan   chan transactionCheck
	dataTypeName      string
	dataTypeVersion   string
	instance          uuid.UUID
	provider          stream.CryptoKeyProvider
	discoveryProvider stream.CryptoKeyProvider
	es                consumer.Consumer[discoveryMetadata[MT]]
	ctx               context.Context
	getKey            func(d MT) string
	discoveryPath     string
	positionKey       []byte
	server            *webserver.Server
}

type metadata[MT any] struct {
	OldId uuid.UUID `json:"old_id"`
	NewId uuid.UUID `json:"new_id"`
	Data  MT        `json:"data"`
}

type action uint8

const (
	invalid action = iota
	update
	query
	response
)

type discoveryMetadata[MT any] struct {
	Key      string    `json:"key"`
	Action   action    `json:"action"`
	Endpoint string    `json:"endpoint"`
	Instance uuid.UUID `json:"instance"`
	Meta     metadata[MT]
}

func Init[DT, MT any](serv *webserver.Server, s stream.Stream, dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, getKey func(d MT) string, ctx context.Context) (ed EventMap[DT, MT], err error) {
	db, err := badger.Open(badger.DefaultOptions("./eventmap/" + dataTypeName).
		WithMaxTableSize(1024 * 1024 * 8).
		WithValueLogFileSize(1024 * 1024 * 8).
		WithValueLogLoadingMode(options.FileIO))
	if err != nil {
		return
	}
	instance, err := uuid.NewV7()
	if err != nil {
		return
	}
	m := mapData[DT, MT]{
		data:              db,
		transactionChan:   make(chan transactionCheck),
		dataTypeName:      dataTypeName,
		dataTypeVersion:   dataTypeVersion,
		instance:          instance,
		provider:          p,
		discoveryProvider: stream.StaticProvider("SkajsFNVOEV81k824LMxO1XCNi+mSpw+HtxKc/e+Xp4="),
		getKey:            getKey,
		discoveryPath:     fmt.Sprintf("/persistentbigdata/%s/:key", dataTypeName),
		server:            serv,
		positionKey:       []byte(fmt.Sprintf("%s_%s_position", s.Name(), dataTypeName)),
		ctx:               ctx,
	}

	serv.Base.GET(m.discoveryPath, func(c *gin.Context) {
		keyStr := c.Param("key")
		if keyStr == "" {
			webserver.ErrorResponse(c, "key not provided", http.StatusNotFound)
			return
		}
		key, err := uuid.FromString(keyStr)
		if err != nil {
			webserver.ErrorResponse(c, "key needs to be a uuid", http.StatusBadRequest)
		}
		var dataByte []byte
		err = m.data.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key.Bytes())
			if err != nil {
				return err
			}

			dataByte, err = item.ValueCopy(nil)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				webserver.ErrorResponse(c, err.Error(), http.StatusNotFound)
				return
			}
			log.WithError(err).Error("while getting data for sync request")
			webserver.ErrorResponse(c, "internal server error", http.StatusInternalServerError)
			return
		}
		var data DT
		err = json.Unmarshal(dataByte, &data)
		if err != nil {
			log.WithError(err).Error("while unmarshalling get request for big data")
			webserver.ErrorResponse(c, "json umarshal error", http.StatusInternalServerError)
			return
		}
		c.JSON(http.StatusOK, data)
		return
	})

	from := store.STREAM_START
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(m.positionKey)
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
	es, err := consumer.New[discoveryMetadata[MT]](s, m.provider, m.ctx)
	if err != nil {
		return
	}
	m.es = es

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
				func() {
					defer e.Acc()
					if e.Type == event.Delete {
						m.delete(e.ReadEvent)
						return
					}
					m.create(e.ReadEvent)
				}()
			}
		}
	}()

	ed = &m
	return
}

func (m *mapData[DT, MT]) create(e event.ReadEvent[discoveryMetadata[MT]]) {
	dmd := e.Data
	if dmd.Action == query {
		if dmd.Instance == m.instance {
			return
		}
		err := m.data.View(func(txn *badger.Txn) error {
			_, err := txn.Get(dmd.Meta.NewId.Bytes())
			return err
		})
		if err != nil {
			return
		}
		u := m.server.Url()
		u.Path = u.Path + strings.ReplaceAll(m.discoveryPath, ":key", dmd.Meta.NewId.String())
		dmd = discoveryMetadata[MT]{
			Key:      dmd.Key,
			Endpoint: u.String(),
			Instance: m.instance,
			Meta:     dmd.Meta,
			Action:   response,
		}

		es := event.Event[discoveryMetadata[MT]]{
			Type: event.Update,
			Data: dmd,
			Metadata: event.Metadata{
				Version:  m.dataTypeVersion,
				DataType: m.dataTypeName,
				Key:      crypto.SimpleHash(dmd.Meta.NewId.String()),
				Extra:    map[string]any{"instance": m.instance},
			},
		}
		we := event.NewWriteEvent(es)
		m.es.Write() <- we
		<-we.Done() //Missing error
		/*
			_, err = m.es.Store(es)
			if err != nil {
				return
			}
		*/
		return
	}
	if dmd.Instance != m.instance {
		err := m.data.View(func(txn *badger.Txn) error {
			_, err := txn.Get(dmd.Meta.NewId.Bytes())
			return err
		})
		if err == nil {
			log.Debug("data already stored")
			return
		}
		_, raw, err := externalImage[DT](dmd.Endpoint)
		if err != nil {
			log.WithError(err).Warning("while getting updated big data") // Should probably use tasks to verify completions instead.
			dmd = discoveryMetadata[MT]{
				Key:      dmd.Key,
				Instance: m.instance,
				Meta:     dmd.Meta,
				Action:   query,
			}

			es := event.Event[discoveryMetadata[MT]]{
				Type: event.Update,
				Data: dmd,
				Metadata: event.Metadata{
					Version:  m.dataTypeVersion,
					DataType: m.dataTypeName,
					Key:      crypto.SimpleHash(dmd.Meta.NewId.String()),
					Extra:    map[string]any{"instance": m.instance},
				},
			}
			we := event.NewWriteEvent(es)
			m.es.Write() <- we
			<-we.Done() //Missing error
			/*
				_, err = m.es.Store(es)
				if err != nil {
					log.WithError(err).Error("unable to store query event after get miss")
					return
				}
			*/
			return
		}
		err = m.data.Update(func(txn *badger.Txn) error {
			return txn.Set(dmd.Meta.NewId.Bytes(), raw)
		})
		if err != nil {
			log.WithError(err).Warning("Update error")
			return
		}
	}
	data, err := json.Marshal(dmd.Meta)
	if err != nil {
		log.WithError(err).Warning("Update error")
		return
	}
	err = m.data.Update(func(txn *badger.Txn) error {
		err = txn.Set([]byte(m.getKey(dmd.Meta.Data)), data)
		if err != nil {
			return err
		}
		if e.Type == event.Update {
			err = txn.Delete(dmd.Meta.OldId.Bytes())
			if err != nil {
				return err
			}
		}

		pos := make([]byte, 8)
		binary.LittleEndian.PutUint64(pos, e.Position)
		return txn.Set(m.positionKey, pos)
	})
	if err != nil {
		log.WithError(err).Warning("Update error")
		return
	}
}

func (m *mapData[DT, MT]) delete(e event.ReadEvent[discoveryMetadata[MT]]) {
	err := m.data.Update(func(txn *badger.Txn) error {

		err := txn.Delete(e.Data.Meta.OldId.Bytes())
		if err != nil {
			return err
		}
		err = txn.Delete([]byte(m.getKey(e.Data.Meta.Data)))
		if err != nil {
			return err
		}

		pos := make([]byte, 8)
		binary.LittleEndian.PutUint64(pos, e.Position)
		return txn.Set(m.positionKey, pos)
	})
	if err != nil {
		log.WithError(err).Warning("Delete error")
	}
}

var ErrKeyNotFound = fmt.Errorf("provided key does not exist")

func (m *mapData[DT, MT]) Get(key string) (data DT, err error) {
	var ed []byte
	err = m.data.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		edt, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		var md metadata[MT]
		err = json.Unmarshal(edt, &md)
		if err != nil {
			return err
		}
		item, err = txn.Get(md.NewId.Bytes())
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
		if errors.Is(err, badger.ErrKeyNotFound) {
			err = ErrKeyNotFound
			return
		}
		return
	}

	err = json.Unmarshal(ed, &data)
	if err != nil {
		return
	}
	return
}

func (m *mapData[DT, MT]) Len() (l int) {
	return len(m.Keys())
}

func (m *mapData[DT, MT]) Keys() (keys []string) {
	keys = make([]string, 0)
	err := m.data.View(func(txn *badger.Txn) error {
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
	if err != nil {
		log.WithError(err).Error("while reading bigdata disc store")
		return
	}
	return
}

func (m *mapData[DT, MT]) Range(f func(key string, data DT) error) {
	err := m.data.View(func(txn *badger.Txn) error {
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
	if err != nil {
		log.WithError(err).Error("while reading bigdata disc store")
		return
	}
}

func (m *mapData[DT, MT]) Exists(key string) (exists bool) {
	_, err := m.Get(key)
	return err == nil
}

func (m *mapData[DT, MT]) Delete(data MT) (err error) {
	var ed []byte
	err = m.data.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(m.getKey(data)))
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

	var md metadata[MT]
	err = json.Unmarshal(ed, &m)
	if err != nil {
		return
	}
	e := event.Event[discoveryMetadata[MT]]{
		Type: event.Delete,
		Data: discoveryMetadata[MT]{
			Meta: md,
		},
		Metadata: event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(md.NewId.String()),
		},
	}

	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() //Missing error
	/*
		_, err = m.es.Store(e)
	*/
	return
}

func (m *mapData[DT, MT]) Set(data DT, meta MT) (err error) {
	log.Trace("Set and wait start")
	newId, err := uuid.NewV7()
	if err != nil {
		return
	}
	eventType := event.Create
	md := metadata[MT]{
		NewId: newId,
		Data:  meta,
	}
	key := m.getKey(meta)

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
	var smd metadata[MT]
	if err == nil {
		err = json.Unmarshal(ed, &smd)
	}
	if err == nil {
		eventType = event.Update
		md.OldId = smd.NewId
	}
	u := m.server.Url()
	u.Path = u.Path + strings.ReplaceAll(m.discoveryPath, ":key", md.NewId.String())
	dmd := discoveryMetadata[MT]{
		Key:      key,
		Endpoint: u.String(),
		Instance: m.instance,
		Meta:     md,
		Action:   update,
	}

	e := event.Event[discoveryMetadata[MT]]{
		Type: eventType,
		Data: dmd,
		Metadata: event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(md.NewId.String()),
			Extra:    map[string]any{"instance": m.instance},
		},
	}
	d, err := json.Marshal(data)
	if err != nil {
		return
	}
	log.Debug("publishing updated data", "endpoint", dmd.Endpoint, "id", md.NewId.String())
	err = m.data.Update(func(txn *badger.Txn) error {
		return txn.Set(md.NewId.Bytes(), d)
	})
	if err != nil {
		return
	}
	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() //Missing error
	/*
		_, err = m.es.Store(e)
		if err != nil {
			return
		}
	*/
	return
}

func externalImage[DT any](url string) (d DT, raw []byte, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	if resp.StatusCode != 200 {
		err = fmt.Errorf("get miss")
		return
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			log.WithError(err).Debug("while closing response body")
		}
	}()
	raw, err = io.ReadAll(resp.Body)
	if err == nil {
		return
	}
	err = json.Unmarshal(raw, &d)
	return
}
