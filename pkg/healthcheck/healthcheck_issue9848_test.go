package healthcheck

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ptypes "github.com/traefik/paerser/types"
	"github.com/traefik/traefik/v3/pkg/api"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/config/label"
	"github.com/traefik/traefik/v3/pkg/config/runtime"
	"github.com/traefik/traefik/v3/pkg/config/static"
	"github.com/traefik/traefik/v3/pkg/server/provider"
	"github.com/traefik/traefik/v3/pkg/testhelpers"
)

type issue9848ServiceResponse struct {
	Name         string                `json:"name"`
	Type         string                `json:"type"`
	LoadBalancer issue9848LoadBalancer `json:"loadBalancer"`
}

type issue9848LoadBalancer struct {
	Servers []struct {
		URL string `json:"url"`
	} `json:"servers"`
	HealthCheck struct {
		Path     string `json:"path"`
		Method   string `json:"method"`
		Port     int    `json:"port"`
		Interval string `json:"interval"`
		Timeout  string `json:"timeout"`
	} `json:"healthCheck"`
}

// issue9848Wire replays the reporter's docker labels through the real label
// decoder used by the Docker provider, serves the resulting runtime
// configuration with the real API handler, and builds the health checker the
// same way the service manager does (pkg/server/service/service.go).
func issue9848Wire(t *testing.T, serviceName string, labels map[string]string) (*dynamic.ServerHealthCheck, issue9848ServiceResponse) {
	t.Helper()

	conf, err := label.DecodeConfiguration(labels)
	require.NoError(t, err)

	// Same qualification the aggregator applies to every provider element
	// (pkg/server/aggregator.go).
	for _, name := range slices.Collect(maps.Keys(conf.HTTP.Services)) {
		conf.HTTP.Services[provider.MakeQualifiedName("docker", name)] = conf.HTTP.Services[name]
		delete(conf.HTTP.Services, name)
	}

	runtimeConf := runtime.NewConfig(*conf)

	server := httptest.NewServer(api.NewBuilder(static.Configuration{API: &static.API{}, Global: &static.Global{}}, nil)(runtimeConf))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/api/http/services/" + serviceName)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var decoded issue9848ServiceResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&decoded))

	svc, found := runtimeConf.Services[serviceName]
	require.True(t, found)
	require.NotNil(t, svc.LoadBalancer)
	require.NotNil(t, svc.LoadBalancer.HealthCheck)

	return svc.LoadBalancer.HealthCheck, decoded
}

func issue9848Checker(t *testing.T, healthCheck *dynamic.ServerHealthCheck) *ServiceHealthChecker {
	t.Helper()

	targets := map[string]*url.URL{"http://172.20.0.2:80": testhelpers.MustParseURL("http://172.20.0.2:80")}

	return NewServiceHealthChecker(t.Context(), nil, healthCheck, nil, nil, http.DefaultTransport, targets, "myservice@docker")
}

// TestIssue9848_NegativeIntervalAPIResponse covers the setup reported in
// issue 9848: the docker label healthcheck.interval=-10s. The API response
// must report the interval that is actually in effect.
func TestIssue9848_NegativeIntervalAPIResponse(t *testing.T) {
	healthCheck, reported := issue9848Wire(t, "myservice@docker", map[string]string{
		"traefik.http.services.myservice.loadbalancer.healthcheck.path":     "/health",
		"traefik.http.services.myservice.loadbalancer.healthcheck.interval": "-10s",
	})

	// Guard: the negative value must have reached the dynamic configuration.
	require.Equal(t, ptypes.Duration(-10*time.Second), healthCheck.Interval)

	shc := issue9848Checker(t, healthCheck)

	assert.Equal(t, shc.interval.String(), reported.LoadBalancer.HealthCheck.Interval)
}

// TestIssue9848_NegativeTimeoutAPIResponse covers the second half of the
// report ("interval or timeout"): the docker label healthcheck.timeout=-10s.
// The API response must report the timeout that is actually in effect.
func TestIssue9848_NegativeTimeoutAPIResponse(t *testing.T) {
	healthCheck, reported := issue9848Wire(t, "myservice@docker", map[string]string{
		"traefik.http.services.myservice.loadbalancer.healthcheck.path":    "/health",
		"traefik.http.services.myservice.loadbalancer.healthcheck.timeout": "-10s",
	})

	require.Equal(t, ptypes.Duration(-10*time.Second), healthCheck.Timeout)

	shc := issue9848Checker(t, healthCheck)

	assert.Equal(t, shc.timeout.String(), reported.LoadBalancer.HealthCheck.Timeout)
}

// TestIssue9848_ValidValuesStayConsistent pins behavior that must not change:
// with plausible values the API response already matches the health checker.
func TestIssue9848_ValidValuesStayConsistent(t *testing.T) {
	healthCheck, reported := issue9848Wire(t, "myservice@docker", map[string]string{
		"traefik.http.services.myservice.loadbalancer.healthcheck.path":     "/health",
		"traefik.http.services.myservice.loadbalancer.healthcheck.interval": "3s",
		"traefik.http.services.myservice.loadbalancer.healthcheck.timeout":  "2s",
	})

	shc := issue9848Checker(t, healthCheck)

	assert.Equal(t, "3s", healthCheck.Interval.String())
	assert.Equal(t, "2s", healthCheck.Timeout.String())
	assert.Equal(t, shc.interval.String(), reported.LoadBalancer.HealthCheck.Interval)
	assert.Equal(t, shc.timeout.String(), reported.LoadBalancer.HealthCheck.Timeout)
}

// TestIssue9848_LaunchProbesConfiguredPath pins behavior that must not
// change: a health checker configured with valid values really probes the
// configured path on the backend and flips the server status to UP.
func TestIssue9848_LaunchProbesConfiguredPath(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	var (
		mu       sync.Mutex
		gotPaths []string
	)

	backend := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		mu.Lock()
		gotPaths = append(gotPaths, req.URL.Path)
		mu.Unlock()
		rw.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	targetURL := testhelpers.MustParseURL(backend.URL)

	healthCheck, reported := issue9848Wire(t, "myservice@docker", map[string]string{
		"traefik.http.services.myservice.loadbalancer.healthcheck.path":     "/health",
		"traefik.http.services.myservice.loadbalancer.healthcheck.interval": "500ms",
		"traefik.http.services.myservice.loadbalancer.healthcheck.timeout":  "400ms",
	})
	require.Equal(t, "500ms", reported.LoadBalancer.HealthCheck.Interval)

	lb := &testLoadBalancer{RWMutex: &sync.RWMutex{}, eventCh: make(chan struct{}, 4)}
	info := &runtime.ServiceInfo{}
	gauge := &testhelpers.CollectingGauge{}

	targets := map[string]*url.URL{targetURL.String(): targetURL}
	shc := NewServiceHealthChecker(ctx, &MetricsMock{gauge}, healthCheck, lb, info, http.DefaultTransport, targets, "myservice@docker")

	var wg sync.WaitGroup
	wg.Go(func() {
		shc.Launch(ctx)
	})

	select {
	case <-lb.eventCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the first health check")
	}
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, gotPaths, "/health")
	assert.Equal(t, map[string]string{targetURL.String(): runtime.StatusUp}, info.GetAllStatus())
}
