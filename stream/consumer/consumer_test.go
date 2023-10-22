package consumer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event/store/inmemory"
	"github.com/cantara/gober/stream/event/store/ondisk"

	"github.com/gofrs/uuid"

	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
)

var c Consumer[dd]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = log.RedactedString("aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0=")
var events = make(map[int]event.ReadEvent[dd])

var STREAM_NAME = "TestConsumer_" + uuid.Must(uuid.NewV7()).String()

type md struct {
	Extra string `json:"extra"`
}

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) log.RedactedString {
	return testCryptKey
}

func TestInit(t *testing.T) {
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	pers, err := ondisk.Init(STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	c, err = New[dd](pers, cryptKeyProvider, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestStoreOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(ctxGlobal)
	defer cancel()
	readEventStream, err := c.Stream(event.AllTypes(), store.STREAM_START, stream.ReadAll(), ctx)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 1; i <= 5; i++ {
		data := dd{
			Id:   i,
			Name: "test",
		}
		meta := md{
			Extra: "extra metadata test",
		}
		e := event.Event[dd]{
			Type: event.Created,
			Data: data,
			Metadata: event.Metadata{
				Extra: map[string]any{"extra": meta.Extra},
			},
		}
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			read := <-readEventStream
			events[read.Data.Id] = read.ReadEvent
			read.Acc()
		}()
		we := event.NewWriteEvent(e)
		c.Write() <- we
		<-we.Done()
		/*
			_, err := c.Store(e)
			if err != nil {
				t.Error(err)
				return
			}
		*/
		wg.Wait()
	}
	return
}

func TestStreamOrder(t *testing.T) {
	for i := 1; i <= 5; i++ {
		e := events[i]
		if e.Type != event.Created {
			t.Error(fmt.Errorf("missmatch event types"))
			return
		}
		if e.Data.Id != i {
			t.Error(fmt.Errorf("missmatch event data id"))
			return
		}
		if e.Data.Name != "test" {
			t.Error(fmt.Errorf("missmatch event data name"))
			return
		}
		if e.Metadata.Extra["extra"] != "extra metadata test" {
			t.Error(fmt.Errorf("missmatch event metadata extra"))
			return
		}
		if e.Metadata.EventType != e.Type {
			t.Error(fmt.Errorf("missmatch event metadata type and event type"))
			return
		}
	}
	return
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}

func BenchmarkStoreAndStream(b *testing.B) {
	//log.SetLevel(log.ERROR) TODO: should add to sbragi
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pers, err := inmemory.Init(fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	c, err := New[[]byte](pers, cryptKeyProvider, ctx)
	if err != nil {
		b.Error(err)
		return
	}
	readEventStream, err := c.Stream(event.AllTypes(), store.STREAM_START, stream.ReadAll(), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	events := make([]event.WriteEventReadStatus[[]byte], b.N)
	go func() {
		for i := 0; i < b.N; i++ {
			e := <-readEventStream
			e.Acc()
			if e.Type != event.Created {
				b.Error(fmt.Errorf("missmatch event types"))
				return
			}
			/*
				if e.Data.Id != events[i].Event().Data.Id {
					b.Error(fmt.Errorf("missmatch event data, %v != %v", e.Data, events[i].Event().Data))
					return
				}
			*/
		}
	}()
	writeEventStream := c.Write()
	for i := 0; i < b.N; i++ {
		/*
			data := dd{
				Id:   i,
				Name: "test" + b.Name(),
			}
		*/
		we := event.NewWriteEvent(event.Event[[]byte]{
			Type: event.Created,
			Data: make([]byte, 1024),
			Metadata: event.Metadata{
				Extra: map[string]any{"extra": "extra metadata test"},
			},
		})
		events[i] = we
		if err != nil {
			b.Error(err)
			return
		}
		writeEventStream <- events[i]
		<-events[i].Done()
	}
}
