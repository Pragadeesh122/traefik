package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/config/runtime"
	"github.com/traefik/traefik/v3/pkg/config/static"
	"github.com/traefik/traefik/v3/pkg/proxy/httputil"
	"github.com/traefik/traefik/v3/pkg/server/service"
	"github.com/traefik/traefik/v3/pkg/tcp"
	th "github.com/traefik/traefik/v3/pkg/testhelpers"
	traefiktls "github.com/traefik/traefik/v3/pkg/tls"
	"golang.org/x/net/http2"
)

// issue8594Backend is the backend from the bug report: it answers 400 without ever
// reading the request body, so the outcome depends solely on how Traefik handles a
// request whose declared Content-Length is larger than the bytes actually sent.
func issue8594Backend(hits *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		hits.Add(1)
		rw.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(rw, "oops something went wrong")
	})
}

// startIssue8594Proxy wires the real routing stack (entry point -> HTTP router ->
// load-balancer service -> reverse proxy) the way a user's configuration does, and
// starts it in the order Traefik does. It returns the entry point address and the
// number of times the backend was reached.
func startIssue8594Proxy(t *testing.T, entryPointName, rule string, withTLS bool) (string, *atomic.Int32) {
	t.Helper()

	var hits atomic.Int32
	backend := httptest.NewServer(issue8594Backend(&hits))
	t.Cleanup(backend.Close)

	entryPointConfig := &static.EntryPoint{}
	entryPointConfig.SetDefaults()
	entryPointConfig.Address = "127.0.0.1:0"

	staticConfig := static.Configuration{
		EntryPoints: map[string]*static.EntryPoint{entryPointName: entryPointConfig},
	}

	tlsManager := traefiktls.NewManager(nil)

	transportManager := service.NewTransportManager(nil)
	transportManager.Update(map[string]*dynamic.ServersTransport{"default@internal": {}})
	proxyBuilder := httputil.NewProxyBuilder(transportManager, nil)

	dialerManager := tcp.NewDialerManager(nil)
	dialerManager.Update(map[string]*dynamic.TCPServersTransport{"default@internal": {}})

	managerFactory := service.NewManagerFactory(staticConfig, nil, nil, transportManager, proxyBuilder, nil, tlsManager)
	routerFactory, err := NewRouterFactory(staticConfig, managerFactory, tlsManager, nil, nil, dialerManager)
	require.NoError(t, err)

	httpConfig := th.BuildConfiguration(
		th.WithRouters(
			th.WithRouter("badrequest",
				th.WithEntryPoints(entryPointName),
				th.WithServiceName("badrequest"),
				th.WithRule(rule))),
		th.WithServices(
			th.WithService("badrequest",
				th.WithServiceServersLoadBalancer(th.WithServers(th.WithServer(backend.URL))))),
	)
	if withTLS {
		httpConfig.Routers["badrequest"].TLS = &dynamic.RouterTLSConfig{}
	}

	// Run the configuration through the same processing Traefik's watcher applies
	// (aggregation injects the default TLS options, then routers' TLS options are
	// resolved), so the entry point terminates TLS with the default certificate.
	merged := mergeConfiguration(dynamic.Configurations{"docker": {HTTP: httpConfig}}, nil)
	merged = applyModel(merged)
	merged.HTTP.Routers = resolveHTTPTLSOptions(merged.HTTP.Routers, false)

	tlsManager.UpdateConfigs(t.Context(), merged.TLS.Stores, merged.TLS.Options, merged.TLS.Certificates)

	routers, _ := routerFactory.CreateRouters(runtime.NewConfig(merged))

	entryPoint, err := NewTCPEntryPoint(t.Context(), entryPointName, entryPointConfig, nil, nil)
	require.NoError(t, err)

	go entryPoint.Start(t.Context())
	entryPoint.SwitchRouter(routers[entryPointName])

	return entryPoint.listener.Addr().String(), &hits
}

// newShortBodyHTTP2Request builds a PUT that declares Content-Length: 5 while the
// body only holds the two bytes "[]" - exactly what `curl --http2 --data '[]'
// -H 'Content-Length: 5'` puts on the wire.
func newShortBodyHTTP2Request(t *testing.T, url string) *http.Request {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, url, nil)
	require.NoError(t, err)

	req.Body = io.NopCloser(strings.NewReader("[]"))
	req.ContentLength = 5
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	return req
}

func http2TLSTransport(addr string) *http.Client {
	dialer := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{"h2"}}}

	return &http.Client{
		Transport: &http2.Transport{
			DialTLSContext: func(ctx context.Context, network, _ string, _ *tls.Config) (net.Conn, error) {
				return dialer.DialContext(ctx, network, addr)
			},
		},
	}
}

func http2CleartextTransport(addr string) *http.Client {
	var dialer net.Dialer

	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, _ string, _ *tls.Config) (net.Conn, error) {
				return dialer.DialContext(ctx, network, addr)
			},
		},
	}
}

// TestIssue8594HTTP2TLSShortBody reproduces the reported setup: an HTTP/2 request
// (TLS, ALPN h2) whose declared Content-Length is larger than the body it actually
// sends. The backend answers 400, so Traefik must forward that 400. On the buggy
// branch Traefik returns 502 Bad Gateway instead, which makes this test fail.
func TestIssue8594HTTP2TLSShortBody(t *testing.T) {
	addr, hits := startIssue8594Proxy(t, "websecure", "Host(`localhost`) && Path(`/badrequest`)", true)

	client := http2TLSTransport(addr)

	resp, err := client.Do(newShortBodyHTTP2Request(t, "https://localhost/badrequest"))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	t.Logf("backend hits=%d body=%q", hits.Load(), string(body))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestIssue8594HTTP2CleartextShortBody is the HTTP/2 (prior-knowledge h2c) variant
// of the same defect: the faulty request reaches the very same Go HTTP/2 server body
// check, so the reverse proxy also turns the backend's 400 into a 502.
func TestIssue8594HTTP2CleartextShortBody(t *testing.T) {
	addr, hits := startIssue8594Proxy(t, "web", "Path(`/badrequest`)", false)

	client := http2CleartextTransport(addr)

	resp, err := client.Do(newShortBodyHTTP2Request(t, "http://localhost/badrequest"))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	t.Logf("backend hits=%d body=%q", hits.Load(), string(body))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestIssue8594HTTP2CorrectBodyPassesThrough guards the normal HTTP/2 path: with a
// consistent Content-Length the backend's 400 must reach the client untouched.
func TestIssue8594HTTP2CorrectBodyPassesThrough(t *testing.T) {
	addr, hits := startIssue8594Proxy(t, "websecure", "Host(`localhost`) && Path(`/badrequest`)", true)

	client := http2TLSTransport(addr)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "https://localhost/badrequest", strings.NewReader("[]"))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	require.EqualValues(t, 1, hits.Load())
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "oops something went wrong", string(body))
}

// TestIssue8594HTTP11ShortBodyIsNotBadGateway guards the HTTP/1.1 path the reporter
// described as working as expected: a request declaring more bytes than it sends must
// not be answered with 502. Traefik blocks waiting for the body instead (the request
// hangs), so the client sees no Bad Gateway.
func TestIssue8594HTTP11ShortBodyIsNotBadGateway(t *testing.T) {
	addr, hits := startIssue8594Proxy(t, "web", "Path(`/badrequest`)", false)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	// Declare five bytes but only send the two bytes "[]", and keep the connection
	// open (no close) so the request never completes - mirroring the faulty client.
	_, err = conn.Write([]byte("PUT /badrequest HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nContent-Type: application/x-www-form-urlencoded\r\n\r\n[]"))
	require.NoError(t, err)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err == nil {
		defer resp.Body.Close()
		assert.NotEqual(t, http.StatusBadGateway, resp.StatusCode)
		return
	}

	// Expected: no response at all within the deadline (the request hangs).
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	assert.True(t, netErr.Timeout())
	t.Logf("backend hits=%d", hits.Load())
}

// TestIssue8594HTTP11NormalRequestPassesThrough guards the plain HTTP/1.1 path.
func TestIssue8594HTTP11NormalRequestPassesThrough(t *testing.T) {
	addr, hits := startIssue8594Proxy(t, "web", "Path(`/badrequest`)", false)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("PUT /badrequest HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n[]"))
	require.NoError(t, err)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	require.EqualValues(t, 1, hits.Load())
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "oops something went wrong", string(body))
}
