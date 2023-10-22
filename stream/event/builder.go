package event

import (
	log "github.com/cantara/bragi/sbragi"
	"github.com/gofrs/uuid"
)

type builder struct {
	Id       uuid.UUID
	Type     Type
	Data     []byte
	Metadata Metadata
}

type Builder interface {
	WithType(t Type) builder
	WithData(data []byte) builder
	WithMetadata(data Metadata) builder
	BuildRead() (ev ByteReadEvent, err error)
	BuildStore() (ev WriteEvent[[]byte], err error)
}

func NewBuilder() Builder {
	return builder{}
}

func (e builder) WithType(t Type) builder {
	e.Type = t
	return e
}

func (e builder) WithData(data []byte) builder {
	e.Data = data
	return e
}

func (e builder) WithMetadata(data Metadata) builder {
	e.Metadata = data
	return e
}

func (e builder) BuildRead() (ev ByteReadEvent, err error) {
	if e.Type == "" {
		log.Error("missing event type in builder")
		err = InvalidTypeError
		return
	}
	e.Metadata.EventType = e.Type
	ev = ByteReadEvent{
		Event: Event[[]byte]{
			Type:     e.Type,
			Data:     e.Data,
			Metadata: e.Metadata,
		},
	}
	return
}

func (e builder) BuildStore() (ev WriteEvent[[]byte], err error) {
	if e.Type == "" {
		log.Error("missing event type in builder")
		err = InvalidTypeError
		return
	}
	if e.Id.IsNil() {
		e.Id, err = uuid.NewV7()
		if err != nil {
			return
		}
	}
	e.Metadata.EventType = e.Type
	ev = WriteEvent[[]byte]{
		event: Event[[]byte]{
			Type:     e.Type,
			Data:     e.Data,
			Metadata: e.Metadata,
		},
	}
	return
}
