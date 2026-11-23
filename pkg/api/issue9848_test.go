package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/label"
	"github.com/traefik/traefik/v3/pkg/config/runtime"
	"github.com/traefik/traefik/v3/pkg/config/static"
	"github.com/traefik/traefik/v3/pkg/server/provider"
)

// TestIssue9848_ServiceRepresentationWithValidHealthCheck pins the JSON the
// API returns for a service whose healthcheck was configured through real
// docker labels with plausible values. This is the representation users rely
// on and it must not change.
func TestIssue9848_ServiceRepresentationWithValidHealthCheck(t *testing.T) {
	labels := map[string]string{
		"traefik.http.services.myservice.loadbalancer.server.url":                  "http://172.20.0.2:80",
		"traefik.http.services.myservice.loadbalancer.healthcheck.path":            "/health",
		"traefik.http.services.myservice.loadbalancer.healthcheck.interval":        "30s",
		"traefik.http.services.myservice.loadbalancer.healthcheck.timeout":         "5s",
		"traefik.http.services.myservice.loadbalancer.healthcheck.method":          "GET",
		"traefik.http.services.myservice.loadbalancer.healthcheck.headers.custom":  "foobar",
		"traefik.http.services.myservice.loadbalancer.healthcheck.followredirects": "true",
		"traefik.http.services.myservice.loadbalancer.passhostheader":              "true",
	}

	conf, err := label.DecodeConfiguration(labels)
	require.NoError(t, err)

	// Same qualification the aggregator applies to every provider element.
	conf.HTTP.Services[provider.MakeQualifiedName("docker", "myservice")] = conf.HTTP.Services["myservice"]
	delete(conf.HTTP.Services, "myservice")

	runtimeConf := runtime.NewConfig(*conf)

	server := httptest.NewServer(NewBuilder(static.Configuration{API: &static.API{}, Global: &static.Global{}}, nil)(runtimeConf))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/api/http/services/myservice@docker")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if os.Getenv("UPDATE_GOLDEN") == "true" {
		require.NoError(t, os.WriteFile("testdata/issue9848-service-valid.json", append(body, '\n'), 0o644))
	}

	expected, err := os.ReadFile("testdata/issue9848-service-valid.json")
	require.NoError(t, err)

	var got, want any
	require.NoError(t, json.Unmarshal(body, &got))
	require.NoError(t, json.Unmarshal(expected, &want))

	assert.Equal(t, want, got)
}
