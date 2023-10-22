package stream

import (
	"context"

	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
)

type Stream interface {
	Write() chan<- store.WriteEvent
	Stream(from store.StreamPosition, ctx context.Context) (out <-chan store.ReadEvent, err error)
	End() (pos uint64, err error)
	Name() string
}

type FilteredStream[T any] interface {
	Write() chan<- event.WriteEventReadStatus[T]
	Store(event event.Event[T]) (position uint64, err error)
	Stream(eventTypes []event.Type, from store.StreamPosition, filter Filter, ctx context.Context) (out <-chan event.ReadEvent[T], err error)
	End() (pos uint64, err error)
	Name() string
}

//FilteredStream(eventTypes []event.Type, from store.StreamPosition, filter Filter[MT], cryptKey CryptoKeyProvider, ctx context.Context) (out <-chan event.Event[DT, any], err error)
