package persistentbigmap

import (
	"context"
	"fmt"
	"github.com/cantara/gober/store/inmemory"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/webserver"
	"os"
	"testing"
	"time"
)

var s stream.Stream
var ed EventMap[dd, dd]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="

var STREAM_NAME = "TestServiceStoreAndStream_" //+ uuid.New().String()

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

func TestPre(t *testing.T) {
	os.RemoveAll("./eventmap/testdata")
	os.RemoveAll("./extraserver/eventmap/testdata")
}

func TestInit(t *testing.T) {
	store, err := inmemory.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	s, err = stream.Init(store, STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	serv, err := webserver.Init(1231)
	if err != nil {
		t.Error(err)
		return
	}
	edt, err := Init[dd, dd](serv, s, "testdata", "1.0.0", cryptKeyProvider, func(d dd) string { return fmt.Sprintf("%d_%s", d.Id, d.Name) }, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	ed = edt
	go serv.Run()
	time.Sleep(10 * time.Second)
	return
}

func TestStore(t *testing.T) {
	data := dd{
		Id:   1,
		Name: "test",
	}
	err := ed.Set(data, data)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestGet(t *testing.T) {
	data, err := ed.Get("1_test")
	fmt.Println(data)
	if err != nil {
		t.Error(err)
		return
	}
	if data.Id != 1 {
		t.Error(fmt.Errorf("missmatch data id"))
		return
	}
	if data.Name != "test" {
		t.Error(fmt.Errorf("missmatch data name"))
		return
	}
}

func TestExtraServer(t *testing.T) {
	err := os.Chdir("extraserver")
	if err != nil {
		t.Error(err)
		return
	}
	serv, err := webserver.Init(1232)
	if err != nil {
		t.Error(err)
		return
	}
	edt, err := Init[dd, dd](serv, s, "testdata", "1.0.0", cryptKeyProvider, func(d dd) string { return fmt.Sprintf("%d_%s", d.Id, d.Name) }, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(10 * time.Second)
	data, err := edt.Get("1_test")
	fmt.Println(data)
	if err != nil {
		t.Error(err)
		return
	}
	if data.Id != 1 {
		t.Error(fmt.Errorf("missmatch data id"))
		return
	}
	if data.Name != "test" {
		t.Error(fmt.Errorf("missmatch data name"))
		return
	}
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}
