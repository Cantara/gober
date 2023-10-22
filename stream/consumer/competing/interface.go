package competing

import (
	"github.com/cantara/gober/stream/event"
)

type Consumer[T any] interface {
	Write() chan<- event.WriteEventReadStatus[T]
	Stream() <-chan event.ReadEventWAcc[T]
	End() (pos uint64, err error)
	Name() string
}
