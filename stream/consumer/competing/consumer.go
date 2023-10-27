package competing

import (
	"context"
	"errors"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/consensus"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
	"github.com/cantara/gober/sync"
	"github.com/gofrs/uuid"
)

type service[T any] struct {
	stream         consumer.Consumer[tm[T]]
	cryptoKey      stream.CryptoKeyProvider
	timeout        time.Duration
	selector       uuid.UUID
	completed      sync.SLK
	competeChan    chan event.ReadEvent[tm[T]]
	selectedOutput chan event.ReadEventWAcc[T]
	writeStream    chan event.WriteEventReadStatus[T]
	cons           consensus.Consensus
	ctx            context.Context
}

type tm[T any] struct {
	Id   uuid.UUID `json:"id"`
	Data T         `json:"data"`
}

func New[T any](s stream.Stream, consBuilder consensus.ConsBuilderFunc, cryptoKey stream.CryptoKeyProvider, from store.StreamPosition, datatype string, timeoutDuration time.Duration, ctx context.Context) (out Consumer[T], err error) {
	fs, err := consumer.New[tm[T]](s, cryptoKey, ctx)
	if err != nil {
		return
	}
	name, err := uuid.NewV7()
	if err != nil {
		return
	}
	cons, err := consBuilder("competing_"+datatype, timeoutDuration+time.Millisecond)
	if err != nil {
		return
	}

	c := service[T]{
		stream:         fs,
		cryptoKey:      cryptoKey,
		timeout:        timeoutDuration,
		selector:       name,
		completed:      sync.NewSLK(),
		competeChan:    make(chan event.ReadEvent[tm[T]], 100),
		selectedOutput: make(chan event.ReadEventWAcc[T], 0),
		writeStream:    make(chan event.WriteEventReadStatus[T], 10),
		cons:           cons,
		ctx:            ctx,
	}
	eventStream, err := c.stream.Stream(event.AllTypes(), from, stream.ReadDataType(datatype), c.ctx)
	if err != nil {
		return
	}
	timeoutChan := make(chan event.ReadEvent[tm[T]], 3)
	go c.readStream(eventStream, timeoutChan)
	go c.readWrites()
	go c.timeoutHandler(timeoutChan)
	go c.competeHandler()

	out = &c
	return
}

func (c *service[T]) timeoutHandler(events <-chan event.ReadEvent[tm[T]]) {
	timeouts := sync.NewMap[context.CancelFunc]()
	for e := range events {
		if e.Type == event.Deleted {
			log.Info("got new event to cancel time out")
			id := e.Data.Id.String()
			cancel, ok := timeouts.Get(id)
			if !ok {
				continue
			}
			timeouts.Delete(id)
			cancel()
			continue
		}
		log.Info("got new event to time out")
		//This is gona use too much ram, however it is okay for now
		ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
		timeouts.Set(e.Data.Id.String(), cancel)
		go func(e event.ReadEvent[tm[T]], ctx context.Context) {
			<-ctx.Done()
			cancel, ok := timeouts.Get(e.Data.Id.String())
			if !ok {
				return
			}
			cancel()
			c.competeChan <- e
		}(e, ctx)
	}
}
func (c *service[T]) compete(e event.ReadEvent[tm[T]]) {
	if c.completed.Get(e.Data.Id.String()) {
		log.Info("already completed, no need to compete", "id", e.Data.Id.String())
		return
	}
	id := e.Data.Id.String()
	log.Info("competing")
	won := c.cons.Request(id) //Might need to difirentiate on won lost and completed
	if !won {
		log.Debug("did not win consesus", "id", id)
		// Add timeout wait
		return
	}
	log.Info("won competition")
	ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
	select {
	case <-ctx.Done():
		c.compete(e)
	case c.selectedOutput <- event.ReadEventWAcc[T]{
		ReadEvent: event.ReadEvent[T]{
			Event: event.Event[T]{
				Type:     e.Type,
				Data:     e.Data.Data,
				Metadata: e.Metadata,
			},
			Position: e.Position,
			Created:  e.Created,
		},
		CTX: ctx,
		Acc: func() {
			cancel()
			we := event.NewWriteEvent[tm[T]](event.Event[tm[T]]{
				Type:     event.Deleted,
				Data:     e.Data,
				Metadata: e.Metadata,
			})
			c.stream.Write() <- we
			status := <-we.Done()
			if status.Error != nil {
				log.WithError(status.Error).Error("while writing completion event")
			}
			c.cons.Completed(id)
		},
	}:
	}
	log.Info("wrote to selectedOutput")
}
func (c *service[T]) competeHandler() {
	for e := range c.competeChan {
		c.compete(e)
	}
}

func (c *service[T]) readStream(events <-chan event.ReadEventWAcc[tm[T]], timeout chan<- event.ReadEvent[tm[T]]) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readStream(events, timeout)
		}
	}()

	es := map[string]struct {
		completed bool
		event     event.ReadEvent[tm[T]]
	}{}
	p := uint64(0)
	end, err := c.stream.End()
	log.WithError(err).Trace("got stream end", "end", end)
	for p < end { //catchup loop, read until no more backpressure
		e := <-events
		switch e.Type {
		case event.Created:
			id := e.Event.Data.Id.String()
			if !es[id].completed {
				es[id] = struct {
					completed bool
					event     event.ReadEvent[tm[T]]
				}{
					completed: false,
					event:     e.ReadEvent,
				}
			}
		case event.Updated:
			//should indicate changes in the event, not sure what to do here.
		case event.Deleted:
			id := e.Event.Data.Id.String()
			v := es[id]
			v.completed = true
			es[id] = v
		}
		e.Acc()
		p = e.Position
	}

	for k, v := range es {
		if v.completed {
			c.completed.Add(k, c.timeout*3)
			continue
		}
		log.Info("competing for backpressure")
		// another dirty hack, should not use go
		//go c.compete(v.event)
		c.competeChan <- v.event
		timeout <- v.event
	}

	log.Info("backpressure finished")

	for e := range events { //Since this stream is controlled by us, we range over it until it closes
		log.Info("range read", "event", e.Data.Id.String())
		e.Acc()
		switch e.Type {
		case event.Created:
			//New event, compete for it
			log.Info("competing for event", "id", e.Data.Id.String())
			//Move competing to a sepparate thread as it will get stuck and should use a stack or something instead to decide
			//Adding go for now as a work around
			//go c.compete(e.ReadEvent)
			c.competeChan <- e.ReadEvent
		case event.Updated:
			//should indicate changes in the event, not sure what to do here.
		case event.Deleted:
			//This should indicate a finished event. Not sure what to do here.
			log.Info("read a completed event", "id", e.Data.Id.String())
			c.completed.Add(e.Data.Id.String(), c.timeout*3)
		}
		log.Info("writing to timeout chan")
		timeout <- e.ReadEvent
		log.Info("wrote to timeout chan")
	}
}

func (c *service[T]) readWrites() {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readWrites()
		}
	}()
	for { //Since this is a read strem not controlled by this process we release it on a ctx done
		select {
		case <-c.ctx.Done():
			return
		case we := <-c.writeStream:
			id, err := uuid.NewV7()
			if err != nil {
				we.Close(store.WriteStatus{
					Error: errors.New("could not create new id"),
				})
				return
			}
			//Seems like it should be possible to do away with this map function, if id is required or generated on the event. But then argh
			we.Event().Type = event.Created
			c.stream.Write() <- event.Map[T, tm[T]](we, func(t T) tm[T] {
				return tm[T]{
					Id:   id,
					Data: t,
				}
			})
			log.Debug("worte mapped event", "id", id)
		}
	}
}

func (c *service[T]) Write() chan<- event.WriteEventReadStatus[T] {
	return c.writeStream
}

func (c *service[T]) Stream() <-chan event.ReadEventWAcc[T] {
	return c.selectedOutput
}

func (c *service[T]) End() (pos uint64, err error) {
	return c.stream.End()
}

func (c *service[T]) Name() string {
	return c.stream.Name()
}
