package eventstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	jsoniter "github.com/json-iterator/go"

	"github.com/gofrs/uuid"

	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
)

var json = jsoniter.ConfigDefault

var es *Stream
var ctx context.Context
var cancel context.CancelFunc
var c *Client

var STREAM_NAME = "TestStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

func TestInit(t *testing.T) {
	var err error
	c, err = NewClient("localhost")
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel = context.WithCancel(context.Background())
	es, err = NewStream(c, STREAM_NAME, ctx)
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
	stream, err := es.Stream(store.STREAM_START, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	e := <-stream
	if e.Type != string(event.Created) {
		t.Error(fmt.Errorf("missmatch event types"))
		return
	}
	return
}

func TestTeardown(t *testing.T) {
	cancel()
	err := c.Close()
	if err != nil {
		t.Error(err)
		return
	}
}

var once sync.Once

func BenchmarkStoreAndStream(b *testing.B) {
	//log.SetLevel(log.ERROR) TODO: Should add to sbragi
	fmt.Println("b.N ", b.N)
	data := make(map[string]interface{})
	data["id"] = 1
	data["name"] = "test"

	bytes, err := json.Marshal(data)
	if err != nil {
		b.Error(err)
		return
	}
	once.Do(func() {
		c, err = NewClient("localhost")
		if err != nil {
			b.Error(err)
			return
		}
	})
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	es, err = NewStream(c, fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
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
		//status := make(chan event.WriteStatus, 1)
		es.Write() <- store.WriteEvent{
			Event: store.Event{
				Id:   uuid.Must(uuid.NewV7()),
				Type: string(event.Created),
				Data: bytes,
			},
			//Status: status,
		}
		/*
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
		*/
	}
	for i := 0; i < b.N; i++ {
		e := <-stream
		if ctx.Err() != nil {
			b.Error(ctx.Err())
		}
		if e.Type != string(event.Created) {
			b.Error(fmt.Errorf("missmatch event types"), e)
			return
		}
		if e.Id.String() == "" {
			b.Error(fmt.Errorf("missing event id"))
			return
		}
	}
}

var esClient *esdb.Client

func BenchmarkEventStore(b *testing.B) {
	fmt.Println("b.N ", b.N)
	data := make(map[string]interface{})
	data["id"] = 1
	data["name"] = "test"

	bytes, err := json.Marshal(data)
	if err != nil {
		b.Error(err)
		return
	}
	once.Do(func() {
		settings, err := esdb.ParseConnectionString("esdb://localhost:2113?tls=false")
		if err != nil {
			b.Error(err)
			return
		}
		esClient, err = esdb.NewClient(settings)
		if err != nil {
			b.Error(err)
			return
		}
	})
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	sn := fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N)
	//stream, err := StreamEventStore(sn, store.STREAM_START, ctx)
	if err != nil {
		b.Error(err)
		return
	}
	writeEvents := make([]esdb.EventData, b.N)
	for i := 0; i < b.N; i++ {
		//status := make(chan event.WriteStatus, 1)
		writeEvents[i] = esdb.EventData{
			EventID:     uuid.Must(uuid.NewV7()),
			ContentType: esdb.BinaryContentType,
			EventType:   string(event.Created),
			Data:        bytes,
		}
	}
	eventChan := make(chan store.ReadEvent, 10000)
	/*
		go func() {
			defer close(eventChan)
			func() {
				stream, err := esClient.SubscribeToStream(ctx, sn, esdb.SubscribeToStreamOptions{
					From: esdb.Start{},
				})
				if err != nil {
					log.WithError(err).Debug("while subscribing to stream ", sn)
					time.Sleep(time.Second)
					return
				}
				defer log.WithError(stream.Close()).Error("Closing stream")
				subscriptionDropped := false
				for !subscriptionDropped {
					select {
					case <-ctx.Done():
						return
					default:
						subEvent := stream.Recv()
						if subEvent.SubscriptionDropped != nil {
							log.WithError(subEvent.SubscriptionDropped.Error).Error("subscription is dropped")
							subscriptionDropped = true
							break
						}

						if subEvent.EventAppeared == nil {
							continue
						}

						e := subEvent.EventAppeared.OriginalEvent()
						ess := store.ReadEvent{
							Event: store.Event{
								Id:       e.EventID,
								Type:     event.TypeFromString(e.EventType),
								Data:     e.Data,
								Metadata: e.UserMetadata,
							},
							//Transaction: e.Position.Commit,
							Position: e.EventNumber + 1,
							Created:  e.CreatedDate,
						}
						eventChan <- ess
					}
				}
			}()
		}()
	*/

	for i := 0; i < b.N/40000+1; i++ {
		to := 40000 * (i + 1)
		if to > b.N {
			to = b.N
		}
		_, err = esClient.AppendToStream(ctx, sn, esdb.AppendToStreamOptions{}, writeEvents[40000*i:to]...)
		if err != nil {
			b.Error(err)
			return
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		stream, err := esClient.SubscribeToStream(ctx, sn, esdb.SubscribeToStreamOptions{
			From: esdb.Start{},
		})
		if err != nil {
			b.Error(err)
			return
		}
		defer func() {
			err := stream.Close()
			if err != nil {
				b.Error(err)
				return
			}
		}()
		for {
			for i := 0; i < b.N; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				subEvent := stream.Recv()
				if subEvent.SubscriptionDropped != nil {
					break
				}

				if subEvent.EventAppeared == nil {
					continue
				}

				e := subEvent.EventAppeared.OriginalEvent()
				if event.TypeFromString(e.EventType) != event.Created {
					b.Error(fmt.Errorf("missmatch event types"), e)
					return
				}
				if e.EventID.String() == "" {
					b.Error(fmt.Errorf("missing event id"))
					return
				}
				ess := store.ReadEvent{
					Event: store.Event{
						Id:       e.EventID,
						Type:     e.EventType,
						Data:     e.Data,
						Metadata: e.UserMetadata,
					},
					//Transaction: e.Position.Commit,
					Position: e.EventNumber + 1,
					Created:  e.CreatedDate,
				}
				eventChan <- ess
			}
		}
	}(ctx)

	for i := 0; i < b.N; i++ {
		<-eventChan
	}
}
