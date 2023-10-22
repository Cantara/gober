package main

import (
	"context"
	"sync"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store/eventstore"
	"github.com/gofrs/uuid"
)

var STREAM_NAME = "BenchmarkConsumer_" + uuid.Must(uuid.NewV7()).String()
var testCryptKey = log.RedactedString("aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0=")
var size = 100000
var eventSize = 1000

func main() {
	//log.SetLevel(log.ERROR) TODO: should add to sbragi
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cl, err := eventstore.NewClient("localhost")
	if err != nil {
		log.WithError(err).Fatal("while creating eventstore client")
		return
	}
	pers, err := eventstore.NewStream(cl, STREAM_NAME, ctx)
	if err != nil {
		log.WithError(err).Fatal("while connecting to storage")
		return
	}
	/*
		pers, err := inmemory.Init(STREAM_NAME, ctx)
		if err != nil {
			log.WithError(err).Fatal("while connecting to storage")
			return
		}
	*/
	c, err := consumer.New[[]byte](pers, stream.StaticProvider(testCryptKey), ctx)
	if err != nil {
		log.WithError(err).Fatal("while creating consumer")
		return
	}
	/*
		readEventStream, err := c.Stream(event.AllTypes(), store.STREAM_START, stream.ReadAll(), ctx)
		if err != nil {
			log.WithError(err).Fatal("while starting stream")
			return
		}
		events := make([]event.WriteEventReadStatus[[]byte], size)
		go func() {
			for i := 0; i < size; i++ {
				e := <-readEventStream
				e.Acc()
				if e.Type != event.Create {
					log.Fatal("missmatch event types")
					return
				}
				/*
					if e.Data.Id != events[i].Event().Data.Id {
						b.Error(fmt.Errorf("missmatch event data, %v != %v", e.Data, events[i].Event().Data))
						return
					}
				/
			}
		}()
	*/
	defer func(start time.Time) {
		fin := time.Now()
		dur := fin.Sub(start)
		eps := float64(size) / float64(dur/time.Second)
		mbps := eps * float64(eventSize) / 1000000
		log.Info("Finished writing events", "number of events", size, "start time", start, "end time", fin,
			"duration", dur, "event size (B)", eventSize, "events / second", eps, "MB/s", mbps)
	}(time.Now())
	//writeEventStream := c.Write()
	we := event.NewWriteEvent(event.Event[[]byte]{
		Type: event.Created,
		Data: make([]byte, eventSize),
		Metadata: event.Metadata{
			Extra: map[string]any{"extra": "extra metadata test"},
		},
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < size; i++ {

			/*
				data := dd{
					Id:   i,
					Name: "test" + b.Name(),
				}
			*/
			//events[i] = we
			c.Write() <- we //events[i]
			//<-events[i].Done()
		}
	}()
	wg.Wait()
}
