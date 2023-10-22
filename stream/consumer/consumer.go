package consumer

import (
	"context"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/mergedcontext"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
	"github.com/gofrs/uuid"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigDefault

type consumer[T any] struct {
	stream             stream.FilteredStream[[]byte]
	cryptoKey          stream.CryptoKeyProvider
	newTransactionChan chan transactionCheck
	currentPosition    uint64
	completables       map[string]transactionCheck
	accChan            chan uint64
	writeStream        chan event.WriteEventReadStatus[T]
	ctx                context.Context
}

type transactionCheck struct {
	position uint64
	complete func()
}

func New[T any](s stream.Stream, cryptoKey stream.CryptoKeyProvider, ctx context.Context) (out Consumer[T], err error) {
	fs, err := stream.Init[[]byte](s, ctx)
	if err != nil {
		return
	}
	c := consumer[T]{
		stream:             fs,
		cryptoKey:          cryptoKey,
		newTransactionChan: make(chan transactionCheck, 0),
		completables:       make(map[string]transactionCheck), //NewMap[transactionCheck](),
		accChan:            make(chan uint64, 0),              //1000),
		writeStream:        make(chan event.WriteEventReadStatus[T], 0),
		ctx:                ctx,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case completable := <-c.newTransactionChan:
				if c.currentPosition >= completable.position {
					completable.complete()
					//close(completeChan.completeChan) // <- struct{}{}
					continue
				}
				c.completables[uuid.Must(uuid.NewV7()).String()] = completable
				//c.completeChans.Store(uuid.Must(uuid.NewV7()).String(), completeChan)
			case position := <-c.accChan:
				if c.currentPosition < position {
					c.currentPosition = position
				}
				for id, completable := range c.completables {
					if position < completable.position {
						continue
					}
					completable.complete()
					//close(completable.completeChan) // <- struct{}{}
					delete(c.completables, id)
				}
			}
		}
	}()

	err = c.streamWriteEvents(c.writeStream)
	if err != nil {
		return
	}

	out = &c
	return
}

func (c *consumer[T]) Write() chan<- event.WriteEventReadStatus[T] {
	return c.writeStream
}

func (c *consumer[T]) Stream(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out <-chan event.ReadEventWAcc[T], err error) {
	return c.streamReadEvents(eventTypes, from, filter, ctx)
}

func (c *consumer[T]) store(e event.WriteEventReadStatus[T]) (position uint64, err error) {
	es, err := EncryptEvent[T](e.Event(), c.cryptoKey)
	if err != nil {
		return
	}

	position, err = c.stream.Store(es)
	if err != nil {
		return
	}
	c.newTransactionChan <- transactionCheck{
		position: position,
		complete: func() {
			e.Close(store.WriteStatus{
				Position: position,
				Time:     time.Now(),
			})
		},
	}
	return
}

func (c *consumer[T]) streamWriteEvents(eventStream <-chan event.WriteEventReadStatus[T]) (err error) {
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case e := <-eventStream:
				p, err := c.store(e)
				log.WithError(err).Debug("store", "pos", p)

			}
		}
	}()
	return
}

func (c *consumer[T]) streamReadEvents(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out <-chan event.ReadEventWAcc[T], err error) {
	mctx, cancel := mergedcontext.MergeContexts(c.ctx, ctx)
	s, err := c.stream.Stream(eventTypes, from, filter, mctx)
	if err != nil {
		cancel()
		return
	}
	eventChan := make(chan event.ReadEventWAcc[T], 0)
	out = eventChan
	go func() {
		defer cancel()
		for {
			select {
			case <-mctx.Done():
				return
			case e := <-s:
				o, err := DecryptEvent[T](e, c.cryptoKey)
				if err != nil {
					log.WithError(err).Error("while reading event")
					continue
				}
				eventChan <- event.ReadEventWAcc[T]{
					ReadEvent: o,
					Acc: func() {
						c.accChan <- o.Position
					},
					CTX: c.ctx,
				}
			}
		}
	}()
	return
}

func (c *consumer[T]) End() (pos uint64, err error) {
	return c.stream.End()
}

func (c *consumer[T]) Name() string {
	return c.stream.Name()
}

func EncryptEvent[T any](e *event.Event[T], cryptoKey stream.CryptoKeyProvider) (es event.Event[[]byte], err error) {
	data, err := json.Marshal(e.Data)
	if err != nil {
		return
	}
	edata, err := crypto.Encrypt(data, cryptoKey(e.Metadata.Key))
	if err != nil {
		return
	}
	ev, err := event.NewBuilder().
		WithType(e.Type).
		WithMetadata(e.Metadata).
		WithData(edata).
		BuildStore()
	if err != nil {
		return
	}
	es = *ev.Event()
	return
}

func DecryptEvent[T any](e event.ReadEvent[[]byte], cryptoKey stream.CryptoKeyProvider) (out event.ReadEvent[T], err error) {
	dataJson, err := crypto.Decrypt(e.Data, cryptoKey(e.Metadata.Key))
	if err != nil {
		log.WithError(err).Warning("Decrypting event data error")
		return
	}
	var data T
	err = json.Unmarshal(dataJson, &data)
	if err != nil {
		log.WithError(err).Warning("Unmarshalling event data error")
		return
	}
	out = event.ReadEvent[T]{
		Event: event.Event[T]{
			Type:     e.Type,
			Data:     data,
			Metadata: e.Metadata,
		},
		Position: e.Position,
		Created:  e.Created,
	}
	return
}
