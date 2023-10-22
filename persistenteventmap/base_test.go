package persistenteventmap

import (
	"context"
	"fmt"
	"os"
	"testing"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/stream/event/store/ondisk"
	"github.com/gofrs/uuid"
)

var ed EventMap[dd]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = log.RedactedString("aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0=")

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) log.RedactedString {
	return testCryptKey
}

func TestPre(t *testing.T) {
	os.RemoveAll("./eventmap/testdata")
}

func TestInit(t *testing.T) {
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	store, err := ondisk.Init(STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	edt, err := Init[dd](store, "testdata", "1.0.0", cryptKeyProvider, func(d dd) string { return fmt.Sprintf("%d_%s", d.Id, d.Name) }, ctxGlobal)
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
	err := ed.Set(data)
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

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}
