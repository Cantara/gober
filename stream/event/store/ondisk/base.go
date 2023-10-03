package ondisk

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/cantara/bragi/sbragi"
	jsoniter "github.com/json-iterator/go"

	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
)

var json = jsoniter.ConfigDefault

type storeEvent struct {
	Event    store.Event
	Position uint64
	Created  time.Time
}

// stream Need to add a way to not store multiple events with the same id in the same stream.
type stream struct {
	db       *os.File
	len      *atomic.Int64
	dbLock   *sync.Mutex
	newData  *sync.Cond
	position uint64
}

type Stream struct {
	data      stream
	name      string
	writeChan chan<- store.WriteEvent
	ctx       context.Context
}

func Init(name string, ctx context.Context) (es *Stream, err error) {
	writeChan := make(chan store.WriteEvent)
	os.Mkdir("streams", 0750)
	f, err := os.OpenFile(fmt.Sprintf("streams/%s", name), os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_SYNC, 0640)
	if err != nil {
		return
	}
	es = &Stream{
		data: stream{
			db:      f,
			len:     &atomic.Int64{},
			dbLock:  &sync.Mutex{},
			newData: sync.NewCond(&sync.Mutex{}),
		},
		name:      name,
		writeChan: writeChan,
		ctx:       ctx,
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-writeChan:
				func() {
					es.data.dbLock.Lock()
					defer es.data.dbLock.Unlock()
					defer func() {
						if e.Status != nil {
							close(e.Status)
						}
					}()
					se := storeEvent{
						Event:    e.Event,
						Position: uint64(es.data.len.Add(1)),
						Created:  time.Now(),
					}
					data, err := json.MarshalToString(se)
					if err != nil {
						log.WithError(err).Error("while marshalling event")
						if e.Status != nil {
							e.Status <- event.WriteStatus{
								Error: err,
							}
						}
						return
					}

					// es.data.db = append(es.data.db, se)
					_, err = es.data.db.WriteString(data + "\n")
					if err != nil {
						log.WithError(err).Error("while writing event to file")
						if e.Status != nil {
							e.Status <- event.WriteStatus{
								Error: err,
							}
						}
						return
					}
					es.data.position = se.Position
					if e.Status != nil {
						e.Status <- event.WriteStatus{
							Time:     se.Created,
							Position: se.Position,
						}
					}

					es.data.newData.Broadcast()
				}()
			}
		}
	}()
	return
}

func (es *Stream) Write() chan<- store.WriteEvent {
	return es.writeChan
}

func (es *Stream) Stream(
	from store.StreamPosition,
	ctx context.Context,
) (out <-chan store.ReadEvent, err error) {
	db, err := os.OpenFile(es.data.db.Name(), os.O_RDONLY, 0640)
	if err != nil {
		return
	}
	eventChan := make(chan store.ReadEvent, 2)
	out = eventChan
	go func() {
		defer close(eventChan)
		for {
			select {
			case <-ctx.Done():
				return
			case <-es.ctx.Done():
				return
			default:
			}
			position := uint64(from)
			if from == store.STREAM_END {
				position = uint64(es.data.len.Load())
			}
			readTo := uint64(0)
			scanner := bufio.NewScanner(db)
			scanner.Buffer(make([]byte, 1<<32), 1<<32)
			scanner.Split(bufio.ScanLines)
			for readTo < position && scanner.Scan() {
				// scanner.Text()
				readTo++
			}
			var se storeEvent
			for {
				select {
				case <-ctx.Done():
					return
				case <-es.ctx.Done():
					return
				default:
					scanner = bufio.NewScanner(db)
					scanner.Split(bufio.ScanLines)
					for scanner.Scan() {
						t := scanner.Text()
						err = json.UnmarshalFromString(t, &se)
						if err != nil {
							log.WithError(err).Fatal("while unmarshalling event from store", "name", es.name, "json", t)
						}
						eventChan <- store.ReadEvent{
							Event:    se.Event,
							Position: se.Position,
							Created:  se.Created,
						}
						readTo++
					}
					if readTo >= uint64(es.data.len.Load()) {
						es.data.newData.L.Lock()
						es.data.newData.Wait()
						es.data.newData.L.Unlock()
					}
				}
			}
		}
	}()
	return
}

func (es *Stream) Name() string {
	return es.name
}

func (es *Stream) End() (pos uint64, err error) {
	pos = es.data.position
	return
}
