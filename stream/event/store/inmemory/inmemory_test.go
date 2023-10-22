package inmemory

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/gofrs/uuid"

	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
)

var es *Stream
var ctx context.Context
var cancel context.CancelFunc

var STREAM_NAME = "TestStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

func TestInit(t *testing.T) {
	ctx, cancel = context.WithCancel(context.Background())
	var err error
	es, err = Init(STREAM_NAME, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestStore(t *testing.T) {
	data := make(map[string]interface{})
	data["id"] = 1
	data["name"] = "test"

	bytes, err := json.Marshal(data)
	if err != nil {
		t.Error(err)
		return
	}
	status := make(chan store.WriteStatus, 1)
	es.Write() <- store.WriteEvent{
		Event: store.Event{
			Id:   uuid.Must(uuid.NewV7()),
			Type: string(event.Created),
			Data: bytes,
		},
		Status: status,
	}
	s := <-status
	if s.Error != nil {
		t.Error(s.Error)
		return
	}
	if s.Time.After(time.Now()) {
		t.Error("write time was after current time")
		return
	}
	if s.Position == 0 {
		t.Error("cannot write at position 0")
		return
	}
	return
}

func TestStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := es.Stream(store.STREAM_START, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	e := <-s
	if e.Type != string(event.Created) {
		t.Error(fmt.Errorf("missmatch inMemEvent types"))
		return
	}
	if e.Id.String() == "" {
		t.Error(fmt.Errorf("missing inMemEvent id"))
		return
	}
	return
}

func TestStoreMultiple(t *testing.T) {
	data := make(map[string]interface{})

	for i := 2; i < 11; i++ {
		data["id"] = i
		data["name"] = "test"

		bytes, err := json.Marshal(data)
		if err != nil {
			t.Error(err)
			return
		}
		status := make(chan store.WriteStatus, 1)
		es.Write() <- store.WriteEvent{
			Event: store.Event{
				Id:   uuid.Must(uuid.NewV7()),
				Type: string(event.Created),
				Data: bytes,
			},
			Status: status,
		}
		s := <-status
		if s.Error != nil {
			t.Error(s.Error)
			return
		}
		if s.Time.After(time.Now()) {
			t.Error("write time was after current time")
			return
		}
		if s.Position == 0 {
			t.Error("cannot write at position 0")
			return
		}
	}
	return
}

func TestStreamMultiple(t *testing.T) {
	s, err := es.Stream(store.STREAM_START, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	var position uint64
	for i := 0; i < 10; i++ {
		e := <-s
		if position < e.Position {
			position = e.Position
		} else {
			t.Errorf("previous transaction id was bigger than current position id. %d >= %d", position, e.Position)
		}
		fmt.Println(e)
		if e.Type != string(event.Created) {
			t.Error(fmt.Errorf("missmatch inMemEvent types"))
			return
		}
		if e.Id.String() == "" {
			t.Error(fmt.Errorf("missing inMemEvent id"))
			return
		}
	}
	return
}

func TestTeardown(t *testing.T) {
	cancel()
}

func BenchmarkStoreAndStream(b *testing.B) {
	//log.SetLevel(log.ERROR) TODO: should add to sbragi
	log.Debug("b.N ", b.N)
	data := make(map[string]interface{})
	data["id"] = 1
	data["name"] = "test"

	bytes, err := json.Marshal(data)
	if err != nil {
		b.Error(err)
		return
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	es, err = Init(fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	stream, err := es.Stream(store.STREAM_START, ctx)
	if err != nil {
		b.Error(err)
		return
	}
	for i := 0; i < b.N; i++ {
		status := make(chan store.WriteStatus, 1)
		es.Write() <- store.WriteEvent{
			Event: store.Event{
				Id:   uuid.Must(uuid.NewV7()),
				Type: string(event.Created),
				Data: bytes,
			},
			Status: status,
		}
		s := <-status
		if s.Error != nil {
			b.Error(s.Error)
			return
		}
		if s.Time.After(time.Now()) {
			b.Error("write time was after current time")
			return
		}
		if s.Position == 0 {
			b.Error("cannot write at position 0")
			return
		}
	}
	for i := 0; i < b.N; i++ {
		e := <-stream
		if e.Type != string(event.Created) {
			b.Error(fmt.Errorf("missmatch inMemEvent types"))
			return
		}
		if e.Id.String() == "" {
			b.Error(fmt.Errorf("missing inMemEvent id"))
			return
		}
	}
}
