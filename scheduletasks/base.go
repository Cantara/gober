package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/tasks"
	"sync"
	"time"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/gofrs/uuid"
)

type Tasks[DT any] interface {
	Create(time.Time, time.Duration, DT) error
}

type transactionCheck struct {
	transaction  uint64
	completeChan chan struct{}
}

type scheduledtasks[DT any] struct {
	name             string
	data             sync.Map
	transactionChan  chan transactionCheck
	eventTypeName    string
	eventTypeVersion string
	timeout          time.Duration
	provider         stream.CryptoKeyProvider
	ctx              context.Context
	es               stream.Stream
	ec               <-chan event.Event[tm[DT]]
	esh              stream.SetHelper
}

type selectionStatus string

const (
	Created  selectionStatus = "created"
	Selected selectionStatus = "selected"
	Finished selectionStatus = "finished"
)

const NoInterval time.Duration = 0

// TaskMetadata temp changed task to be the id that is used for strong and id seems to now only be used for events.
type TaskMetadata struct {
	Id       uuid.UUID       `json:"id"`
	Task     uuid.UUID       `json:"task"` //Needs to be added because both sheduledtask and task can run in the same stream.
	Next     uuid.UUID       `json:"next_id"`
	NextTask uuid.UUID       `json:"next_task_id"`
	After    time.Time       `json:"after"`
	Interval time.Duration   `json:"interval"`
	Status   selectionStatus `json:"status"`
}

type tm[DT any] struct {
	Task     DT
	Metadata TaskMetadata
}

func Init[DT any](s stream.Stream, tsks tasks.Tasks[DT], dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, execute func(DT) bool, ctx context.Context) (ed Tasks[DT], err error) {
	name, err := uuid.NewV7()
	if err != nil {
		return
	}
	eventChan, err := stream.NewStream[tm[DT]](s, event.AllTypes(), store.STREAM_START,
		stream.ReadDataType(dataTypeName), p, ctx)
	if err != nil {
		return
	}
	t := scheduledtasks[DT]{
		name:             name.String(),
		data:             sync.Map{},
		transactionChan:  make(chan transactionCheck),
		eventTypeName:    dataTypeName,
		eventTypeVersion: dataTypeVersion,
		provider:         p,
		ctx:              ctx,
		es:               s,
		ec:               eventChan,
	}

	createdTasksChan := make(chan uuid.UUID, 10)
	go func() {
		for id := range createdTasksChan {
			taskAny, loaded := t.data.Load(id)
			if !loaded {
				log.Warning("tried loading " + id.String() + " but failed. unable to operate on the new task.")
				continue
			}
			task := taskAny.(tm[DT])
			if task.Metadata.Status != Created {
				log.Info("task already in or passed the selection stage. " + id.String())
				continue
			}
			//This can create very many go routines, but it is a very simple solution.
			go func() {
				defer func() {
					err := recover()
					if err != nil {
						log.AddError(err.(error)).Error("panic recovered while creating scheduled task")
					}
				}()
				waitingFor := task.Metadata.After.Sub(time.Now())
				log.Debug("Waiting for ", waitingFor.Minutes(), " minutes")
				time.Sleep(waitingFor)
				err := tsks.Add(task.Metadata.Task, task.Task)
				if err != nil {
					log.AddError(err).Error("while creating scheduled task")
					return
				}
				nextid, err := uuid.NewV7()
				if err != nil {
					log.AddError(err).Crit("while creating uuid for next action in scheduled task")
					return
				}
				e, err := t.event(task.Metadata.Next, event.Update, tm[DT]{
					Task: task.Task,
					Metadata: TaskMetadata{
						Id:       task.Metadata.Id,
						Task:     task.Metadata.Task,
						Next:     nextid,
						NextTask: task.Metadata.NextTask,
						Status:   Selected,
						After:    task.Metadata.After,
						Interval: task.Metadata.Interval,
					},
				})
				if err != nil {
					log.AddError(err).Error("while creating event for next action in scheduled task")
					return
				}
				err = t.esh.SetAndWait(e)
				if err != nil {
					log.AddError(err).Error("while storing event for next action in scheduled task")
					return
				}
			}()
		}
	}()

	t.esh = stream.InitSetHelper(func(e event.Event[tm[DT]]) {
		t.data.Store(e.Data.Metadata.Task, e.Data)
		if e.Type == event.Create {
			createdTasksChan <- e.Data.Metadata.Task
		}
	}, func(e event.Event[tm[DT]]) {
		t.data.Delete(e.Data.Metadata.Task)
	}, t.es, t.provider, t.ec, t.ctx)

	go func() {
		for {
			tsk, err := tsks.Select()
			if err != nil {
				if errors.Is(err, tasks.NothingToSelectError) {
					time.Sleep(500 * time.Millisecond)
					continue
				}
				log.AddError(err).Crit("while selection task")
				continue
			}
			go func() {
				defer func() {
					err := recover()
					if err != nil {
						log.AddError(err.(error)).Crit("panic while executing")
						return
					}
				}()
				log.Debug("selected task: ", tsk)
				// This tsk is the one from tasks not scheduled tasks, thus the id is not the one that is used to store with here.
				taskAny, loaded := t.data.Load(tsk.Id)
				if !loaded {
					log.Warning("tried loading " + tsk.Id.String() + " but failed. unable to execute task.")
					return
				}
				task := taskAny.(tm[DT])
				if task.Metadata.Status != Selected {
					log.Info("task in impossible stage when selected for execution. " + tsk.Id.String())
					return
				}
				if !execute(tsk.Data) {
					log.Error("there was an error while executing task. not finishing")
					return
				}
				log.Debug("executed task:", task)
				terr := tsks.Finish(task.Metadata.Task)
				if terr != nil {
					log.AddError(terr).Crit("while finishing task")
				}

				if task.Metadata.Interval != NoInterval {
					err = t.create(task.Metadata.NextTask, task.Metadata.After.Add(task.Metadata.Interval), task.Metadata.Interval, task.Task)
					if err != nil {
						log.AddError(err).Crit("while creating event for finished action in scheduled task")
						return
					}
				}

				e, err := t.event(task.Metadata.Next, event.Delete, tm[DT]{
					Task: tsk.Data, // not sure if i want to use the one from the select or the base from scheduletask
					Metadata: TaskMetadata{
						Id:       task.Metadata.Id,
						Task:     task.Metadata.Task,
						NextTask: task.Metadata.NextTask,
						Status:   Finished,
						After:    task.Metadata.After,
						Interval: task.Metadata.Interval,
					},
				})
				if err != nil {
					log.AddError(err).Error("while creating event for finished action in scheduled task")
					return
				}
				err = t.esh.SetAndWait(e)
				if err != nil {
					log.AddError(err).Error("while storing event for finished action in scheduled task")
					return
				}
			}()
		}
	}()

	ed = &t
	return
}

func (t *scheduledtasks[DT]) event(id uuid.UUID, eventType event.Type, data tm[DT]) (e event.StoreEvent, err error) {
	data.Metadata.Next = uuid.Must(uuid.NewV7())
	return event.NewBuilder[tm[DT]]().
		WithId(id).
		WithType(eventType).
		WithData(data).
		WithMetadata(event.Metadata{
			Version:  t.eventTypeVersion,
			DataType: t.eventTypeName,
			Key:      crypto.SimpleHash(id.String()),
			Extra: map[string]any{
				"select_status": data.Metadata.Status,
			},
		}).
		BuildStore()
}

func (t *scheduledtasks[DT]) Create(a time.Time, i time.Duration, dt DT) (err error) {
	id, err := uuid.NewV7()
	if err != nil {
		return
	}
	return t.create(id, a, i, dt)
}

func (t *scheduledtasks[DT]) create(id uuid.UUID, a time.Time, i time.Duration, dt DT) (err error) {
	taskId, err := uuid.NewV7()
	if err != nil {
		return
	}
	nextId, err := uuid.NewV7()
	if err != nil {
		return
	}
	nextTaskId, err := uuid.NewV7()
	if err != nil {
		return
	}
	e, err := t.event(id, event.Create, tm[DT]{
		Task: dt,
		Metadata: TaskMetadata{
			Id:       id,
			Task:     taskId,
			Next:     nextId,
			NextTask: nextTaskId,
			Status:   Created,
			After:    a,
			Interval: i,
		},
	})
	if err != nil {
		return
	}
	err = t.esh.SetAndWait(e)
	return
}

func printJson(v any) {
	o, _ := json.MarshalIndent(v, "", "  ")
	log.Println(string(o))
}

var NothingToSelectError = fmt.Errorf("nothing to select")
