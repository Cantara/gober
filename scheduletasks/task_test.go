package tasks

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/consensus"
	"github.com/cantara/gober/discovery/local"
	"github.com/cantara/gober/stream/event/store/inmemory"
	"github.com/gofrs/uuid"
)

var ts Tasks[dd]
var ts2 Tasks[dd]
var ts3 Tasks[dd]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = log.RedactedString("aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0=")
var td dd
var wg sync.WaitGroup
var count int

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) log.RedactedString {
	return testCryptKey
}

var ct *testing.T

var intervalChan = make(chan struct{})

func executeFunc(d dd) bool {
	log.Info("Executed", "data", d, "count", count)
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
	if count == 6 {
		close(intervalChan)
	}
	//time.Sleep(10 * time.Second)

	time.Sleep(5 * time.Second)
	defer wg.Done()
	return true
}

func TestInit(t *testing.T) {

	//dl, _ := log.NewDebugLogger()
	//dl.SetDefault()
	ct = t
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	store, err := inmemory.Init(STREAM_NAME, ctxGlobal)
	//store, err := eventstore.Init()
	if err != nil {
		t.Error(err)
		return
	}
	token := "someTestToken"
	p, err := consensus.Init(3134, token, local.New())
	if err != nil {
		t.Fatal(err)
	}
	edt, err := Init[dd](store, p.AddTopic, "testdata_schedule1", "1.0.0", cryptKeyProvider, executeFunc, time.Second*15, false, 5, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	ts2, err = Init[dd](store, p.AddTopic, "testdata_schedule2", "1.0.0", cryptKeyProvider, executeFunc, time.Second*15, true, 5, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	ts3, err = Init[dd](store, p.AddTopic, "testdata_schedule3", "1.0.0", cryptKeyProvider, executeFunc, time.Second*15, false, 5, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	ts = edt
	go p.Run()
	time.Sleep(time.Microsecond)
	log.Info("init done")
	return
}

func TestCreate(t *testing.T) {
	log.Info("creating single event")
	ct = t
	data := dd{
		Id:   1,
		Name: "test",
	}
	wg.Add(1)
	err := ts.Create("test_task_1", time.Now(), NoInterval, data)
	if err != nil {
		t.Error(err)
		return
	}
	log.Info("created single event")
	return
}

func TestFinish(t *testing.T) {
	log.Info("finishing single event")
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
	log.Info("finishied single event")
}

func TestCreateIntervalWithCatchup(t *testing.T) {
	log.Info("creating first task in reacurring task chain")
	ct = t
	data := dd{
		Id:   1,
		Name: "test_interval",
	}
	wg.Add(5)
	err := ts.Create("test_task_recurring", time.Now().Add(-time.Second*100), 10*time.Second, data)
	if err != nil {
		t.Error(err)
		return
	}
	log.Info("created first task in reacurring task chain")
	return
}

func TestFinishInterval(t *testing.T) {
	log.Info("staring to wait for reacurring tast to be executed 15 times")
	ct = t
	select {
	case <-time.After(60 * time.Second):
		t.Error("timeout on task finish")
	case <-intervalChan:
		wg.Wait()
	}
	log.Info("done waiting for reacurring tast to be executed 15 times")
}

func TestTairdown(t *testing.T) {
	ct = t
	ctxGlobalCancel()
}

var benchRun = 0

func BenchmarkTasks_Create_Select_Finish(b *testing.B) {
	benchRun++
	//log.SetLevel(log.ERROR) TODO: should add to sbragi
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	store, err := inmemory.Init(fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	token := "someTestToken"
	p, err := consensus.Init(uint16(3143+benchRun), token, local.New())
	if err != nil {
		b.Fatal(err)
	}

	edt, err := Init[dd](store, p.AddTopic, "testdata", "1.0.0", cryptKeyProvider, func(d dd) bool { log.Info("benchmark task ran", "data", d); return true }, time.Second, false, 10, ctx) //FIXME: There seems to be an issue with reusing streams
	if err != nil {
		b.Error(err)
		return
	}
	go p.Run()
	data := dd{
		Id:   1,
		Name: "test",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = edt.Create(fmt.Sprintf("bench_task_%d", i), time.Now(), NoInterval, data)
		if err != nil {
			b.Error(err)
			return
		}
	}
}
