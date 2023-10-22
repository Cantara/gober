package event

import (
	"context"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/stream/event/store"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigDefault

type Metadata struct {
	Stream    string         `json:"stream"`
	EventType Type           `json:"event_type"`
	Version   string         `json:"version"`
	DataType  string         `json:"data_type"`
	Key       string         `json:"key"` //Strictly used for things like getting the cryptoKey
	Extra     map[string]any `json:"extra"`
	Created   time.Time      `json:"created"`
}

type Event[T any] struct {
	Type     Type     `json:"type"`
	Data     T        `json:"data"`
	Metadata Metadata `json:"metadata"`
}

type ReadEvent[T any] struct {
	Event[T]

	Position uint64    `json:"position"`
	Created  time.Time `json:"created"`
}

type ReadEventWAcc[T any] struct {
	ReadEvent[T]

	Acc func()
	CTX context.Context
}

type WriteEvent[T any] struct {
	event  Event[T]
	Status chan store.WriteStatus
}

type WriteEventReadStatus[T any] interface {
	Event() *Event[T]
	Done() <-chan store.WriteStatus
	Close(store.WriteStatus)
	Store() *store.WriteEvent
	StatusChan() chan store.WriteStatus
}

func Map[OT, NT any](e WriteEventReadStatus[OT], f func(OT) NT) WriteEventReadStatus[NT] {
	return &WriteEvent[NT]{
		event: Event[NT]{
			Type:     e.Event().Type,
			Data:     f(e.Event().Data),
			Metadata: e.Event().Metadata,
		},
		Status: e.StatusChan(),
	}
}

func NewWriteEvent[T any](e Event[T]) WriteEventReadStatus[T] { //Dont think i like this
	return &WriteEvent[T]{
		event:  e,
		Status: make(chan store.WriteStatus, 1),
	}
}

func (e *WriteEvent[T]) Event() *Event[T] {
	return &e.event
}

func (e *WriteEvent[T]) Done() <-chan store.WriteStatus {
	return e.Status
}

func (e *WriteEvent[T]) Close(status store.WriteStatus) {
	if e.Status == nil {
		return
	}
	e.Status <- status
	close(e.Status)
}

func (e *WriteEvent[T]) StatusChan() chan store.WriteStatus {
	return e.Status
}

func (e *WriteEvent[T]) Store() *store.WriteEvent {
	mByte, err := json.Marshal(e.event.Metadata)
	if err != nil {
		log.WithError(err).Error("while marshaling metadata")
		e.Close(store.WriteStatus{
			Error: err,
		})
		return nil
	}
	dByte, err := json.Marshal(e.event.Data)
	if err != nil {
		log.WithError(err).Error("while marshaling data")
		e.Close(store.WriteStatus{
			Error: err,
		})
		return nil
	}
	return &store.WriteEvent{
		Event: store.Event{
			Type:     string(e.event.Type),
			Data:     dByte,
			Metadata: mByte,
		},
		Status: e.Status,
	}
}

type ByteEvent Event[[]byte]
type ByteWriteEvent WriteEvent[[]byte]
type ByteReadEvent ReadEvent[[]byte]
