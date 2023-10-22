package consumer

import (
	"context"

	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
)

type Consumer[T any] interface {
	Write() chan<- event.WriteEventReadStatus[T]
	Stream(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out <-chan event.ReadEventWAcc[T], err error)
	End() (pos uint64, err error)
	Name() string
}
