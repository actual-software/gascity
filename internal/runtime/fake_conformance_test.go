package runtime_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/runtime/runtimetest"
)

func TestFakeConformance(t *testing.T) {
	fp := runtime.NewFake()
	var counter int64

	runtimetest.RunProviderTests(t, func(_ *testing.T) (runtime.Provider, runtime.Config, string) {
		id := atomic.AddInt64(&counter, 1)
		name := fmt.Sprintf("fake-conform-%d", id)
		return fp, runtime.Config{}, name
	})
}
