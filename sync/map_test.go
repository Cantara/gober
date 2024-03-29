package sync

import (
	"fmt"
	"sync"
	"testing"
)

var smap Map[int]

func TestNew(t *testing.T) {
	smap = NewMap[int]()
	if smap == nil {
		t.Error("unable to create map")
		return
	}
}

func TestSyncMap_Set(t *testing.T) {
	smap.Set("key", 19)
}

func TestSyncMap_Get(t *testing.T) {
	i, ok := smap.Get("key")
	if !ok {
		t.Error("get not okay")
		return
	}
	if i != 19 {
		t.Error("data not equal, expected 19 got ", i)
		return
	}
}

func TestSyncMap_GetMap(t *testing.T) {
	m := smap.GetMap()
	for k, v := range m {
		if k != "key" {
			t.Error("key if not key, ", k)
			return
		}
		if v != 19 {
			t.Error("data not equal, expected 19 got ", v)
		}
	}
}

var bsmap = NewMap[int]()

func BenchmarkMultiReadWrite(b *testing.B) {
	go func() {
		v, _ := bsmap.Get("k")
		bsmap.Set("k", v+1)
	}()
	var wg sync.WaitGroup
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		go func(i int) {
			defer wg.Done()
			m := bsmap.GetMap()
			for k, v := range m {
				bsmap.Set(fmt.Sprintf("%v-%v", k, v), i)
			}
		}(i)
	}
	wg.Wait()
	m := bsmap.GetMap()
	for k, v := range m {
		fmt.Println(k, v)
	}
}
