package tasks

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/stream/consumer/competing"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
	"github.com/gofrs/uuid"
)

type Tasks[DT any] interface {
	Create(time.Time, time.Duration, DT) error
	Tasks() (tasks []TaskMetadata)
}

type scheduledtasks[DT any] struct {
	eventTypeName    string
	eventTypeVersion string
	timeout          time.Duration
	provider         stream.CryptoKeyProvider
	ctx              context.Context
	es               competing.Consumer[tm[DT]]
	taskLock         sync.Mutex
	tasks            []TaskMetadata
}

const NoInterval time.Duration = 0

// TaskMetadata temp changed task to be the id that is used for strong and id seems to now only be used for events.
type TaskMetadata struct {
	Id       uuid.UUID     `json:"id"`
	After    time.Time     `json:"after"`
	Interval time.Duration `json:"interval"`
}

type tm[DT any] struct {
	Task     DT
	Metadata TaskMetadata
	ctx      context.Context
	cancel   context.CancelFunc
}

func Init[DT any](s stream.Stream, dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, execute func(DT) bool, workers int, ctx context.Context) (ed Tasks[DT], err error) {
	dataTypeName = dataTypeName + "_scheduled"
	es, err := competing.New[tm[DT]](s, p, store.STREAM_START, dataTypeName, time.Minute*15, ctx)
	if err != nil {
		return
	}
	t := scheduledtasks[DT]{
		eventTypeName:    dataTypeName,
		eventTypeVersion: dataTypeVersion,
		ctx:              ctx,
		es:               es,
	}
	esTasks, err := competing.New[tm[DT]](s, p, store.STREAM_START, dataTypeName+"_executor", time.Second*30, ctx)
	if err != nil {
		return
	}

	go func() {
		for {
			select {
			case <-t.ctx.Done():
				return
			case e := <-es.Stream():
				t.taskLock.Lock()
				i := contains(e.Data.Metadata.Id, t.tasks)
				if i >= 0 {
					t.tasks[i] = e.Data.Metadata
				} else {
					t.tasks = append(t.tasks, e.Data.Metadata)
				}
				t.taskLock.Unlock()
				go func() {
					from, to, waitTime := e.Data.Metadata.After, time.Now(), e.Data.Metadata.After.Sub(time.Now())
					log.Trace(fmt.Sprintf("waiting until it is time to do work, from %v to %v with waiting time of %v", from, to, waitTime),
						"from", from, "to", to, "wait_time", waitTime)
					select {
					case <-t.ctx.Done():
						return
					case <-e.CTX.Done():
						return
					case <-time.After(e.Data.Metadata.After.Sub(time.Now())):
					}
					we := event.NewWriteEvent(event.Event[tm[DT]]{
						Type: event.Created,
						Data: e.Data,
						Metadata: event.Metadata{
							Version:  t.eventTypeVersion,
							DataType: t.eventTypeName + "_executor",
							Key:      crypto.SimpleHash(e.Data.Metadata.Id.String()),
						},
					})
					esTasks.Write() <- we
					ws := <-we.Done()
					if ws.Error != nil {
						log.WithError(ws.Error).Error("while creating scheduled task")
						return
					}
					e.Acc()
				}()
			}
		}
	}()

	//Should probably move this out to an external function created by the user instead. For now adding a customizable worker pool size
	for i := 0; i < workers; i++ {
		go func() {
			for {
				select {
				case <-t.ctx.Done():
					return
				case e := <-esTasks.Stream():
					func() {
						defer func() {
							err := recover()
							if err != nil {
								log.WithError(fmt.Errorf("%v", err)).Error("panic while executing")
								return
							}
						}()
						log.Debug("selected task", "event", e)
						// Should be fixed now; This tsk is the one from tasks not scheduled tasks, thus the id is not the one that is used to store with here.
						if !execute(e.Data.Task) {
							log.Warning("there was an error while executing task. not finishing")
							return
						}
						log.Debug("executed task", "event", e)
						if e.Data.Metadata.Interval != NoInterval {
							err = t.create(e.Data.Metadata.Id, e.Data.Metadata.After.Add(e.Data.Metadata.Interval), e.Data.Metadata.Interval, e.Data.Task)
							if err != nil {
								log.WithError(err).Error("while creating next event for finished action in scheduled task with interval")
								return
							}
						}
						e.Acc()
					}()
				}
			}
		}()
	}

	ed = &t
	return
}

func (t *scheduledtasks[DT]) event(eventType event.Type, data tm[DT]) (e event.Event[tm[DT]], err error) {
	e = event.Event[tm[DT]]{
		Type: eventType,
		Data: data,
		Metadata: event.Metadata{
			Version:  t.eventTypeVersion,
			DataType: t.eventTypeName,
			Key:      crypto.SimpleHash(data.Metadata.Id.String()),
		},
	}
	return
}

// To finish adding updatable tasks, should add task"name" and use that to store the task. Thus also checking if the that that is sent to delete is the one stored. Incase the next task comes before the delete-action for some reason.
func (t *scheduledtasks[DT]) Create(a time.Time, i time.Duration, dt DT) (err error) {
	return t.create(uuid.Must(uuid.NewV7()), a, i, dt)
}

func (t *scheduledtasks[DT]) create(id uuid.UUID, a time.Time, i time.Duration, dt DT) (err error) {
	e, err := t.event(event.Created, tm[DT]{
		Task: dt,
		Metadata: TaskMetadata{
			After:    a,
			Interval: i,
			Id:       id,
		},
	})
	if err != nil {
		return
	}
	we := event.NewWriteEvent(e)
	t.es.Write() <- we
	ws := <-we.Done()
	return ws.Error
}

func (t *scheduledtasks[DT]) Tasks() (tasks []TaskMetadata) {
	t.taskLock.Lock()
	defer t.taskLock.Unlock()
	tasks = make([]TaskMetadata, len(t.tasks))
	copy(tasks, t.tasks)
	return
}

func contains(id uuid.UUID, tasks []TaskMetadata) int {
	idb := id.Bytes()
	for i, t := range tasks {
		if !bytes.Equal(t.Id.Bytes(), idb) {
			continue
		}
		return i
	}
	return -1
}
