package competing

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
	"github.com/cantara/gober/stream/event/store/ondisk"
)

var c Consumer[dd]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = log.RedactedString("aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0=")
var events = make(map[int]event.ReadEventWAcc[dd])

var STREAM_NAME = "TestCompetingConsumer_" + uuid.Must(uuid.NewV7()).String()

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
	c, err = New[dd](pers, cryptKeyProvider, store.STREAM_START, "datatype", time.Second*15, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestStoreOrder(t *testing.T) {
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
				Extra:    map[string]any{"extra": meta.Extra},
				DataType: "datatype",
			},
		}
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("reading event")
			read := <-c.Stream()
			log.Info("read event", "event", read)
			events[i] = read
			read.Acc()
		}()

		we := event.NewWriteEvent(e)
		c.Write() <- we
		status := <-we.Done()
		if status.Error != nil {
			t.Error(status.Error)
			return
		}
		/*
			_, err := c.Store(e)
			if err != nil {
				t.Error(err)
				return
			}
		*/
		log.Info("waiting for event to read")
		wg.Wait()
	}
	return
}

func TestStreamOrder(t *testing.T) {
	for i := 1; i <= 5; i++ {
		e := events[i]
		log.Info("events", "events", e)
		if e.Data.Id != i {
			t.Error(fmt.Errorf("missmatch event data id: %d != %d", e.Data.Id, i))
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

func TestTimeout(t *testing.T) {
	data := dd{
		Id:   10,
		Name: "test_timeout",
	}

	e := event.Event[dd]{
		Type: event.Created,
		Data: data,
		Metadata: event.Metadata{
			DataType: "datatype",
		},
	}

	we := event.NewWriteEvent(e)
	c.Write() <- we
	status := <-we.Done()
	if status.Error != nil {
		t.Error(status.Error)
		return
	}
	/*
		c.Write() <- event.WriteEvent[dd]{
			Event: e,
		}
			_, err := c.Store(e)
			if err != nil {
				t.Error(err)
				return
			}
	*/
	log.Info("reading event to discard")
	read := <-c.Stream()
	log.Info("read", "event", read)
	log.Info("waiting until after timeout (20s)")
	time.Sleep(time.Second * 20)
	//log.Info("accing event after timeout and it should have been discarded") //This and the next one should probably not be true anymore :/
	//read.Acc()

	log.Info("reading event to acc")
	select {
	case read = <-c.Stream():
	default:
		t.Error("task was not ready to select")
		return
	}
	log.Info("read", "event", read)
	read.Acc()
	log.Info("verifying there is no extra events a peering (40s)")
	select {
	case <-time.After(40 * time.Second):
	case read = <-c.Stream():
		t.Error("task still existed after timeout: ", read)
		return
	}
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}
