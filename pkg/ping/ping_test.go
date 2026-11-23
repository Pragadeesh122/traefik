package ping

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServeHTTP(t *testing.T) {
	h := &Handler{}
	h.SetDefaults()

	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusOK, recorder.Code)

	ctx, cancel := context.WithCancel(t.Context())
	h.WithContext(ctx)
	cancel()

	require.Eventually(t, func() bool {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
		return recorder.Code == h.TerminatingStatusCode
	}, time.Second, 10*time.Millisecond)
}

func TestServeHTTPConcurrentWithCancel(t *testing.T) {
	// Guards the data race between the terminating flag written on ctx.Done
	// and the per-request reads in ServeHTTP; meaningful under -race.
	h := &Handler{}
	h.SetDefaults()

	ctx, cancel := context.WithCancel(t.Context())
	h.WithContext(ctx)

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		})
	}
	cancel()
	wg.Wait()
}
