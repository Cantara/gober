package persistenteventmap

import (
	"context"
	"fmt"
	"github.com/cantara/gober/store/inmemory"
	"testing"
)

var ed EventMap[dd, md]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="

var STREAM_NAME = "TestServiceStoreAndStream_" //+ uuid.New().String()

type md struct {
	Extra string `json:"extra"`
}

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

func TestInit(t *testing.T) {
	store, err := inmemory.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	edt, err := Init[dd, md](store, "testdata", "1.0.0", STREAM_NAME, "create_setandwait", "update_setandwait", "delete_setandwait", cryptKeyProvider, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	ed = edt
	return
}

func TestStore(t *testing.T) {
	data := dd{
		Id:   1,
		Name: "test",
	}
	meta := md{
		Extra: "extra metadata test",
	}
	err := ed.Set("1_test", data, meta)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestGet(t *testing.T) {
	data, meta, err := ed.Get("1_test")
	fmt.Println(data)
	fmt.Println(meta)
	if err != nil {
		t.Error(err)
		return
	}
	if meta.Type != "create_setandwait" {
		t.Error(fmt.Errorf("Missmatch event types"))
		return
	}
	if data.Id != 1 {
		t.Error(fmt.Errorf("Missmatch data id"))
		return
	}
	if data.Name != "test" {
		t.Error(fmt.Errorf("Missmatch data name"))
		return
	}
	if meta.Event.Extra != "extra metadata test" {
		t.Error(fmt.Errorf("Missmatch event metadata extra"))
		return
	}
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
	ed.Close()
}