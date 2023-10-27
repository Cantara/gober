package consensus

import (
	"testing"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/discovery/local"
)

var c Consensus

func TestInit(t *testing.T) {
	dl, _ := log.NewDebugLogger()
	dl.SetDefault()
	log.Info("Initializing consensus")
	token := "someTestToken"
	topic := "test"
	p, err := Init(3132, token, local.New())
	if err != nil {
		t.Fatal(err)
	}
	c, err = p.AddTopic(topic, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	go p.Run()
	time.Sleep(time.Second)
}

func TestConsensus(t *testing.T) {
	if !c.Request("id1") {
		t.Fatal("did not win request")
	}
}

func TestTimeout(t *testing.T) {
	if !c.Request("id2") {
		t.Fatal("did not win request")
	}
	if c.Request("id2") {
		t.Fatal("did win request before timeout")
	}
	time.Sleep(time.Second)
	if !c.Request("id2") {
		t.Fatal("did not win request after timeout")
	}
}

func TestCompletion(t *testing.T) {
	if !c.Request("id3") {
		t.Fatal("did not win request")
	}
	c.Completed("id3")
	if c.Request("id3") {
		t.Fatal("did win request after completion")
	}
	time.Sleep(time.Second)
	if c.Request("id3") {
		t.Fatal("did win request after completion and timeout")
	}
}
