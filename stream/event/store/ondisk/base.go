package ondisk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/cantara/bragi/sbragi"
	jsoniter "github.com/json-iterator/go"

	"github.com/cantara/gober/stream/event/store"
)

var json = jsoniter.ConfigFastest

const (
	B  = 1
	KB = B << 10
	MB = KB << 10
	GB = MB << 10
)

type storeEvent struct {
	Event    store.Event
	Position uint64
	Created  time.Time
}

// stream Need to add a way to not store multiple events with the same id in the same stream.
type stream struct {
	db  *os.File
	len *atomic.Int64
	//dbLock   *sync.Mutex
	newData  *sync.Cond
	position uint64
}

type Stream struct {
	data      stream
	name      string
	writeChan chan<- store.WriteEvent
	ctx       context.Context
}

func Init(name string, ctx context.Context) (s *Stream, err error) {
	writeChan := make(chan store.WriteEvent)
	os.Mkdir("streams", 0750)
	f, err := os.OpenFile(fmt.Sprintf("streams/%s", name), os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_SYNC, 0640)
	if err != nil {
		return
	}
	s = &Stream{
		data: stream{
			db:  f,
			len: &atomic.Int64{},
			//dbLock:  &sync.Mutex{},
			newData: sync.NewCond(&sync.Mutex{}),
		},
		name:      name,
		writeChan: writeChan,
		ctx:       ctx,
	}
	go writeStrem(s, writeChan)
	return
}

func writeStrem(s *Stream, writes <-chan store.WriteEvent) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		log.WithError(fmt.Errorf("%v", r)).Error("recovering write stream", "stream", s.name)
		writeStrem(s, writes)
	}()
	stream := json.NewEncoder(s.data.db)
	for {
		select {
		case <-s.ctx.Done():
			return
		case e := <-writes:
			func() {
				defer func() {
					if e.Status != nil {
						close(e.Status)
					}
				}()
				se := storeEvent{
					Event:    e.Event,
					Position: uint64(s.data.len.Add(1)),
					Created:  time.Now(),
				}
				err := stream.Encode(se)
				if err != nil {
					log.WithError(err).Error("while writing event to file")
					if e.Status != nil {
						e.Status <- store.WriteStatus{
							Error: err,
						}
					}
					return
				}
				s.data.position = se.Position
				if e.Status != nil {
					e.Status <- store.WriteStatus{
						Time:     se.Created,
						Position: se.Position,
					}
				}

				s.data.newData.Broadcast()
			}()
		}
	}
}

func (s *Stream) Write() chan<- store.WriteEvent {
	return s.writeChan
}

func (s *Stream) Stream(from store.StreamPosition, ctx context.Context) (out <-chan store.ReadEvent, err error) {
	eventChan := make(chan store.ReadEvent, 2)
	out = eventChan
	go readStream(s, eventChan, uint64(from), ctx)
	return
}

func readStream(s *Stream, events chan<- store.ReadEvent, position uint64, ctx context.Context) {
	exit := false
	defer func() {
		if exit {
			close(events)
		}
	}()
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		log.WithError(fmt.Errorf("%v", r)).Error("recovering read stream", "stream", s.name)
		readStream(s, events, position, ctx)
	}()
	db, err := os.OpenFile(fmt.Sprintf("streams/%s", s.name), os.O_RDONLY, 0640)
	//db, err := os.OpenFile(es.data.db.Name(), os.O_RDONLY, 0640)
	if err != nil {
		log.WithError(err).Fatal("while opening stream file")
		return
	}
	defer db.Close()
	stream := json.NewDecoder(db)
	select {
	case <-ctx.Done():
		exit = true
		return
	case <-s.ctx.Done():
		exit = true
		return
	default:
	}
	//position := uint64(from)
	if position == uint64(store.STREAM_END) {
		position = uint64(s.data.len.Load())
	}
	readTo := uint64(0)
	var se storeEvent
	for readTo < position {
		err := stream.Decode(&se)
		if err != nil {
			if errors.Is(err, io.EOF) {
				time.Sleep(time.Millisecond * 250)
				continue
			}
			log.WithError(err).Fatal("while unmarshalling event from store catchup", "name", s.name)
		}
		readTo = se.Position
	}
	for !exit {
		log.Trace("starting new reader loop", "name", s.name)
		select {
		case <-ctx.Done():
			exit = true
			return
		case <-s.ctx.Done():
			exit = true
			return
		default:
			for stream.More() {
				log.Trace("has more", "name", s.name)
				err := stream.Decode(&se)
				if err != nil {
					/*Should not happen
					if errors.Is(err, io.EOF) {
						time.Sleep(time.Millisecond * 250)
						continue
					}
					*/
					log.WithError(err).Fatal("while unmarshalling event from store", "name", s.name)
				}
				events <- store.ReadEvent{
					Event:    se.Event,
					Position: se.Position,
					Created:  se.Created,
				}
				position = se.Position
			}
			log.Trace("empty checking if there has come new data", "name", s.name, "p", position, "dl", s.data.len.Load(), "dp", s.data.position)
			if position >= uint64(s.data.len.Load()) {
				log.Trace("waiting for new data", "name", s.name)
				s.data.newData.L.Lock()
				s.data.newData.Wait()
				s.data.newData.L.Unlock()
			} else {
				time.Sleep(time.Millisecond * 250)
			}
			stream = json.NewDecoder(db)
		}
	}
}

func (s *Stream) Name() string {
	return s.name
}

func (s *Stream) End() (pos uint64, err error) {
	pos = s.data.position
	return
}
