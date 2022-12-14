package stream

import (
	"context"
	"github.com/cantara/gober/stream/event"

	"github.com/cantara/gober/store"
)

type Persistence interface {
	Store(streamName string, ctx context.Context, events ...store.Event) (transactionId uint64, err error)
	Stream(streamName string, from store.StreamPosition, ctx context.Context) (out <-chan store.Event, err error)
}

type Stream interface {
	Store(event event.StoreEvent, cryptoKey CryptoKeyProvider) (transactionId uint64, err error)
	Stream(from store.StreamPosition, ctx context.Context) (out <-chan store.Event, err error)
	Name() string
}

//Stream(eventTypes []event.Type, from store.StreamPosition, filter Filter[MT], cryptKey CryptoKeyProvider, ctx context.Context) (out <-chan event.Event[DT, any], err error)
