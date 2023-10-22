package competing

import (
	"bytes"
	"context"
	"errors"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
	"github.com/cantara/gober/syncmap"
	"github.com/gofrs/uuid"
)

type service[T any] struct {
	stream         consumer.Consumer[tm[T]]
	cryptoKey      stream.CryptoKeyProvider
	selected       syncmap.SyncMap[event.ReadEvent[tm[T]]]
	finished       syncmap.SyncMap[struct{}]
	timeout        time.Duration
	selector       uuid.UUID
	selectedOutput chan event.ReadEventWAcc[T]
	writeStream    chan event.WriteEventReadStatus[T]
	ctx            context.Context
}

type tm[T any] struct {
	Id       uuid.UUID `json:"id"`
	Data     T         `json:"data"`
	Timeout  time.Time `json:"timeout"`
	Selector uuid.UUID `json:"selector"`
}

func New[T any](s stream.Stream, cryptoKey stream.CryptoKeyProvider, from store.StreamPosition, datatype string, timeoutDuration time.Duration, ctx context.Context) (out Consumer[T], err error) {
	//fs, err := stream.Init[[]byte](s, ctx)
	fs, err := consumer.New[tm[T]](s, cryptoKey, ctx)
	if err != nil {
		return
	}
	name, err := uuid.NewV7()
	if err != nil {
		return
	}

	//What is she staring at?

	c := service[T]{
		stream:         fs,
		cryptoKey:      cryptoKey,
		selected:       syncmap.New[event.ReadEvent[tm[T]]](),
		finished:       syncmap.New[struct{}](),
		timeout:        timeoutDuration,
		selector:       name,
		selectedOutput: make(chan event.ReadEventWAcc[T], 0),
		writeStream:    make(chan event.WriteEventReadStatus[T], 10),
		ctx:            ctx,
	}
	eventStream, err := c.stream.Stream(event.AllTypes(), from, stream.ReadDataType(datatype), c.ctx)
	if err != nil {
		return
	}

	selectable, timeout, finished, selected := c.startReadStream(eventStream)
	go c.readSelectables(selectable)
	go c.readTimeout(timeout, selectable)
	go c.readFinished(finished)

	go c.readSelected(selected, c.selectedOutput)
	go c.readWrites(c.writeStream)

	out = &c
	return
}

func (c *service[T]) Write() chan<- event.WriteEventReadStatus[T] {
	return c.writeStream
}

func (c *service[T]) Stream() <-chan event.ReadEventWAcc[T] {
	return c.selectedOutput
}

func (c *service[T]) readSelectables(selectable <-chan event.ReadEvent[tm[T]]) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readSelectables(selectable)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case e := <-selectable: //This needs some kind of throttling
			if e.Metadata.Created.Add(30 * time.Second).Before(time.Now()) {
				go func() {
					time.Sleep(time.Minute)
					if _, isFinished := c.finished.Get(e.Data.Id.String()); isFinished {
						return
					}
					selected, isSelected := c.selected.Get(e.Data.Id.String())
					if isSelected && selected.Data.Timeout.After(time.Now()) {
						return
					}
					c.compete(e.Event)
				}()
				continue
			}
			c.compete(e.Event)
		}
	}
}

func (c *service[T]) readTimeout(timeout <-chan event.ReadEvent[tm[T]], selectable chan<- event.ReadEvent[tm[T]]) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readTimeout(timeout, selectable)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case newTimeout := <-timeout:
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Error("recovered", "error", r)
					}
				}()
				id := newTimeout.Data.Id
				if newTimeout.Metadata.Created.Add(30 * time.Second).Before(time.Now()) { //This needs to be verified.
					time.Sleep(time.Minute)
					if _, isFinished := c.finished.Get(id.String()); isFinished {
						return
					}
					selected, isSelected := c.selected.Get(id.String())
					if isSelected && selected.Data.Timeout.After(time.Now()) {
						return
					}
				}
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(newTimeout.Data.Timeout.Sub(time.Now())):
					if _, finished := c.finished.Get(id.String()); finished {
						return
					}
					selected, ok := c.selected.Get(id.String())
					if !ok {
						log.Error("This should never be true, neither finished nor selected but timed out ", id)
						return
					}
					if !selected.Metadata.Created.Equal(newTimeout.Metadata.Created) {
						return
					}
					//log.Debug("TIMED OUT!! ", id)
					selectable <- newTimeout
				}
			}()
		}
	}
}

func (c *service[T]) readSelected(selected <-chan event.ReadEvent[tm[T]], out chan<- event.ReadEventWAcc[T]) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readSelected(selected, out)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			close(out)
			return
		case current := <-selected:
			currentCTX, cancel := context.WithTimeout(c.ctx, c.timeout-time.Now().Sub(current.Metadata.Created)-(time.Second*5))
			select {
			case <-c.ctx.Done():
				return
			case <-currentCTX.Done():
				//log.Debug("write timeout ", current.Data.ID)
			case out <- event.ReadEventWAcc[T]{
				ReadEvent: event.ReadEvent[T]{
					Event: event.Event[T]{
						Type:     current.Type,
						Data:     current.Data.Data,
						Metadata: current.Metadata,
					},
					Position: current.Position,
				},
				Acc: func() {
					defer cancel()

					c.stream.Write() <- event.NewWriteEvent(event.Event[tm[T]]{
						Type:     event.Deleted,
						Data:     current.Data,
						Metadata: current.Metadata,
					})
					/*
						bes, err := EncryptEvent[tm[T]](event.Event[tm[T]]{
							Type:     event.Delete,
							Data:     current.Data,
							Metadata: current.Metadata,
						}, c.cryptoKey)
						if err != nil {
							log.WithError(err).Error("while encrypting acc event")
							return
						}
						c.stream.Write() <- event.ByteWriteEvent{
							Event: event.Event[[]byte](bes),
						}
							if err != nil {
								log.WithError(err).Error("while storing finished competing consumer")
								return
							}
					*/
				},
				CTX: currentCTX,
			}:
				//log.Debug("wrote ", current.Data.ID)
			}
		}
	}
}

func (c *service[T]) readFinished(finished <-chan event.ReadEvent[tm[T]]) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readFinished(finished)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-finished: //Not sure what to do here
		}
	}
}

func (c *service[T]) startReadStream(eventStream <-chan event.ReadEventWAcc[tm[T]]) (selectable chan event.ReadEvent[tm[T]], timeout, finished <-chan event.ReadEvent[tm[T]], selected <-chan event.ReadEvent[tm[T]]) {
	selectableChan := make(chan event.ReadEvent[tm[T]], 10)
	timeoutChan := make(chan event.ReadEvent[tm[T]], 10)
	finishedChan := make(chan event.ReadEvent[tm[T]], 10)
	selectedChan := make(chan event.ReadEvent[tm[T]], 10)

	go c.readStream(eventStream, selectableChan, timeoutChan, finishedChan, selectedChan)

	selectable = selectableChan
	selected = selectedChan
	timeout = timeoutChan
	finished = finishedChan
	return
}

func (c *service[T]) readStream(eventStream <-chan event.ReadEventWAcc[tm[T]], selectable, timeout, finished chan<- event.ReadEvent[tm[T]], selected chan<- event.ReadEvent[tm[T]]) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readStream(eventStream, selectable, timeout, finished, selected)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			close(selectable)
			close(timeout)
			close(finished)
			close(selected)
			log.Debug("CLOSED CHANNELS !!!!")
			return
		case e := <-eventStream:
			func() {
				defer e.Acc()
				/*
					e, err := consumer.DecryptEvent[tm[T]](ce, c.cryptoKey)
					if err != nil {
						log.WithError(err).Error("while reading event")
						continue
					}
				*/
				id := e.Data.Id
				if e.Type == event.Deleted {
					c.finished.Set(id.String(), struct{}{})
					finished <- e.ReadEvent //Delete from selected
					return
				}
				_, isFinished := c.finished.Get(id.String())
				if isFinished {
					log.Debug("skipping event since it is finished, ", id)
					return
				}
				if e.Type == event.Created {
					selectable <- e.ReadEvent
					return
				}
				if time.Now().After(e.Data.Timeout) {
					//If this is a new server and catchup is too slow, the event could in theory time out before we get back to it.
					//  Currently considering that highly unlikely.
					return
				}
				stored, isSelected := c.selected.Get(id.String())
				if isSelected && !e.Metadata.Created.After(stored.Data.Timeout) {
					return

				}
				//log.Debug("storing ", e.Data.Id, " in runner ", c.selector, " with winner ", e.Data.Selector)
				c.selected.Set(id.String(), e.ReadEvent)
				timeout <- e.ReadEvent
				//log.Info(e.Data.Selector, " ", c.selector, " ", e.Data.Id)
				if bytes.Equal(e.Data.Selector.Bytes(), c.selector.Bytes()) {
					///log.Info(e.Data.Selector, " == ", c.selector)
					selected <- e.ReadEvent
					return

				}
			}()
		}
	}
}

func (c *service[T]) readWrites(events <-chan event.WriteEventReadStatus[T]) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readWrites(events)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case we := <-events:
			id, err := uuid.NewV7()
			if err != nil {
				we.Close(store.WriteStatus{
					Error: errors.New("could not create new id"),
				})
				return
			}
			c.stream.Write() <- event.Map[T, tm[T]](we, func(t T) tm[T] {
				return tm[T]{
					Id:   id,
					Data: t,
				}
			})
		}
	}
}

func (c *service[T]) End() (pos uint64, err error) {
	return c.stream.End()
}

func (c *service[T]) Name() string {
	return c.stream.Name()
}

func (c *service[T]) compete(e event.Event[tm[T]]) {
	e.Type = event.Updated
	e.Data.Selector = c.selector
	e.Data.Timeout = time.Now().Add(c.timeout)

	we := event.NewWriteEvent[tm[T]](e)
	c.stream.Write() <- we
	s := <-we.Done()
	if s.Error != nil {
		log.WithError(s.Error).Warning("while trying to compete")
		return
	}
}
