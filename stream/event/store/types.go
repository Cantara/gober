package store

import (
	"math"
	"time"

	"github.com/gofrs/uuid"
)

type Event struct {
	Id       uuid.UUID `json:"id"`
	Type     string    `json:"type"`
	Data     []byte    `json:"data"`
	Metadata []byte    `json:"metadata"`
}

type ReadEvent struct {
	Event

	Position uint64    `json:"position"`
	Created  time.Time `json:"created"`
}

type WriteEvent struct {
	Event

	Status chan<- WriteStatus
}

type WriteStatus struct {
	Error    error
	Position uint64
	Time     time.Time
}

type StreamPosition uint64

const (
	STREAM_START StreamPosition = 0
	STREAM_END   StreamPosition = math.MaxUint64
)
