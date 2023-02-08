package tasks

import (
	"context"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/store/inmemory"
	"github.com/cantara/gober/stream"
	"github.com/gofrs/uuid"
	"sync"
	"testing"
	"time"
)

var ts Tasks[dd]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
var td dd
var wg sync.WaitGroup
var count int

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

var ct *testing.T

func TestInit(t *testing.T) {
	ct = t
	store, err := inmemory.Init()
	//store, err := eventstore.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	s, err := stream.Init(store, STREAM_NAME, ctxGlobal)
	if err != nil {
		return
	}
	edt, err := Init[dd](s, "testdata_schedule", "1.0.0", cryptKeyProvider, func(d dd) bool {
		log.Println("Executed after time ", d, " with count ", count)
		if count > 16 {
			ctxGlobalCancel()
			ct.Error("catchup ran too many times")
			ct.FailNow()
			return true
		}
		if d.Name == "test" && count != 0 {
			ctxGlobalCancel()
			ct.Error("task ran more than once")
			ct.FailNow()
			return false
		}
		count++
		if count == 2 {
			return false
		}
		if count%2 == 0 {
			//return false
		}
		//time.Sleep(10 * time.Second)

		time.Sleep(5 * time.Second)
		defer wg.Done()
		return true
	}, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	ts = edt
	return
}

func TestCreate(t *testing.T) {
	ct = t
	data := dd{
		Id:   1,
		Name: "test",
	}
	wg.Add(1)
	err := ts.Create(time.Now(), NoInterval, data)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestFinish(t *testing.T) {
	ct = t
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-time.After(30 * time.Second):
		t.Error("timeout on task finish")
	case <-c:
	}
}

func TestCreateIntervalWithCatchup(t *testing.T) {
	ct = t
	data := dd{
		Id:   1,
		Name: "test_interval",
	}
	wg.Add(15)
	err := ts.Create(time.Now().Add(-time.Second*100), 10*time.Second, data)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestFinishInterval(t *testing.T) {
	ct = t
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-time.After(110 * time.Second):
		t.Error("timeout on task finish")
	case <-c:
	}
}

func TestTairdown(t *testing.T) {
	ct = t
	ctxGlobalCancel()
}

func BenchmarkTasks_Create_Select_Finish(b *testing.B) {
	log.SetLevel(log.ERROR)
	store, err := inmemory.Init()
	if err != nil {
		b.Error(err)
		return
	}
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	s, err := stream.Init(store, fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		return
	}

	edt, err := Init[dd](s, "testdata", "1.0.0", cryptKeyProvider, func(d dd) bool { log.Println(d); return true }, ctx) //FIXME: There seems to be an issue with reusing streams
	if err != nil {
		b.Error(err)
		return
	}
	data := dd{
		Id:   1,
		Name: "test",
	}
	for i := 0; i < b.N; i++ {
		err = edt.Create(time.Now(), NoInterval, data)
		if err != nil {
			b.Error(err)
			return
		}
	}
}
