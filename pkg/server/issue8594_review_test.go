package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	"golang.org/x/net/http2/hpack"
)

const issue8594ReviewPrefix = "request declared a Content-Length of "

// issue8594ReviewBackend answers 400 without reading the request body.
func issue8594ReviewBackend(hits *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		hits.Add(1)
		rw.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(rw, "oops something went wrong")
	})
}

// issue8594ReviewCapture records the errors the servers transport returns from
// RoundTrip: these are the very values the reverse proxy hands to the error
// handler and thus to ComputeStatusCode.
type issue8594ReviewCapture struct {
	mu   sync.Mutex
	errs []error
}

func (c *issue8594ReviewCapture) record(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

func (c *issue8594ReviewCapture) get() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errs
}

type issue8594ReviewRoundTripper struct {
	rt      http.RoundTripper
	capture *issue8594ReviewCapture
}

func (r *issue8594ReviewRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.rt.RoundTrip(req)
	if err != nil {
		r.capture.record(err)
	}
	return resp, err
}

type issue8594ReviewTransportManager struct {
	httputil.TransportManager

	capture *issue8594ReviewCapture
}

func (m issue8594ReviewTransportManager) GetRoundTripper(name string) (http.RoundTripper, error) {
	rt, err := m.TransportManager.GetRoundTripper(name)
	if err != nil {
		return nil, err
	}
	return &issue8594ReviewRoundTripper{rt: rt, capture: m.capture}, nil
}

// issue8594ReviewStart wires the real routing stack like issue8594_test.go does, but
// with a caller-provided backend URL and servers transport, and with the RoundTripper
// used by the reverse proxy wrapped to record the errors ComputeStatusCode will see.
func issue8594ReviewStart(t *testing.T, backendURL string, entryPointTLS bool, serversTransport *dynamic.ServersTransport) (string, *issue8594ReviewCapture) {
	t.Helper()

	capture := &issue8594ReviewCapture{}

	entryPointName := "websecure"
	rule := "Host(`localhost`) && Path(`/badrequest`)"
	if !entryPointTLS {
		entryPointName = "web"
		rule = "Path(`/badrequest`)"
	}

	entryPointConfig := &static.EntryPoint{}
	entryPointConfig.SetDefaults()
	entryPointConfig.Address = "127.0.0.1:0"

	staticConfig := static.Configuration{
		EntryPoints: map[string]*static.EntryPoint{entryPointName: entryPointConfig},
	}

	tlsManager := traefiktls.NewManager(nil)

	transportManager := service.NewTransportManager(nil)
	transportManager.Update(map[string]*dynamic.ServersTransport{"default@internal": serversTransport})
	proxyBuilder := httputil.NewProxyBuilder(issue8594ReviewTransportManager{TransportManager: transportManager, capture: capture}, nil)

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
				th.WithServiceServersLoadBalancer(th.WithServers(th.WithServer(backendURL))))),
	)
	if entryPointTLS {
		httpConfig.Routers["badrequest"].TLS = &dynamic.RouterTLSConfig{}
	}

	merged := mergeConfiguration(dynamic.Configurations{"docker": {HTTP: httpConfig}}, nil)
	merged = applyModel(merged)
	merged.HTTP.Routers = resolveHTTPTLSOptions(merged.HTTP.Routers, false)

	tlsManager.UpdateConfigs(t.Context(), merged.TLS.Stores, merged.TLS.Options, merged.TLS.Certificates)

	routers, _ := routerFactory.CreateRouters(runtime.NewConfig(merged))

	entryPoint, err := NewTCPEntryPoint(t.Context(), entryPointName, entryPointConfig, nil, nil)
	require.NoError(t, err)

	go entryPoint.Start(t.Context())
	entryPoint.SwitchRouter(routers[entryPointName])

	return entryPoint.listener.Addr().String(), capture
}

func issue8594ReviewShortBodyRequest(t *testing.T, url string) *http.Request {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, url, nil)
	require.NoError(t, err)

	req.Body = io.NopCloser(strings.NewReader("[]"))
	req.ContentLength = 5
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	return req
}

func issue8594ReviewHTTP2Client(addr string) *http.Client {
	dialer := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{"h2"}}}

	return &http.Client{
		Transport: &http2.Transport{
			DialTLSContext: func(ctx context.Context, network, _ string, _ *tls.Config) (net.Conn, error) {
				return dialer.DialContext(ctx, network, addr)
			},
		},
	}
}

// TestIssue8594ReviewCleartextServersTransportShortBody runs the reported faulty
// request (HTTP/2 over TLS, Content-Length: 5, 2-byte body) against a cleartext
// backend: the servers transport surfaces the mismatch as a "readfrom" *net.OpError,
// the only shape the const comment in proxy.go describes.
func TestIssue8594ReviewCleartextServersTransportShortBody(t *testing.T) {
	var hits atomic.Int32
	backend := httptest.NewServer(issue8594ReviewBackend(&hits))
	t.Cleanup(backend.Close)

	addr, capture := issue8594ReviewStart(t, backend.URL, true, &dynamic.ServersTransport{})

	resp, err := issue8594ReviewHTTP2Client(addr).Do(issue8594ReviewShortBodyRequest(t, "https://localhost/badrequest"))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.LessOrEqual(t, hits.Load(), int32(1))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	errs := capture.get()
	t.Logf("captured %d error(s)", len(errs))
	for _, captured := range errs {
		t.Logf("captured error type %T: %v", captured, captured)

		var opErr *net.OpError
		require.ErrorAs(t, captured, &opErr)
		assert.Equal(t, "readfrom", opErr.Op)
		assert.True(t, strings.HasPrefix(opErr.Err.Error(), issue8594ReviewPrefix), "wrapped error is %q", opErr.Err)
	}
}

// TestIssue8594ReviewTLSServersTransportShortBody runs the reported faulty request
// against a TLS backend through the servers transport. The transport has no
// "readfrom" TCP fast path against a TLS backend: the mismatch error reaches
// ComputeStatusCode unwrapped, so only the raw-prefix fallback of
// isContentLengthMismatch turns it into the forwarded 400 asserted here (without the
// fix, this configuration answers 500).
func TestIssue8594ReviewTLSServersTransportShortBody(t *testing.T) {
	var hits atomic.Int32
	backend := httptest.NewUnstartedServer(issue8594ReviewBackend(&hits))
	backend.StartTLS()
	t.Cleanup(backend.Close)

	addr, capture := issue8594ReviewStart(t, backend.URL, true, &dynamic.ServersTransport{InsecureSkipVerify: true})

	resp, err := issue8594ReviewHTTP2Client(addr).Do(issue8594ReviewShortBodyRequest(t, "https://localhost/badrequest"))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	t.Logf("backend hits=%d body=%q", hits.Load(), string(body))

	assert.LessOrEqual(t, hits.Load(), int32(1))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	errs := capture.get()
	t.Logf("captured %d error(s)", len(errs))
	for _, captured := range errs {
		t.Logf("captured error type %T: %v", captured, captured)

		assert.True(t, strings.HasPrefix(captured.Error(), issue8594ReviewPrefix), "captured=%v (%T)", captured, captured)
	}
}

// shortBodyErrorReader yields the two bytes "[]" and then fails the way the inbound
// Go HTTP/2 server fails a body whose declared Content-Length was not reached
// (net/http/internal/http2/server.go, (*stream).endStream).
type shortBodyErrorReader struct {
	read bool
}

func (r *shortBodyErrorReader) Read(p []byte) (int, error) {
	if r.read {
		return 0, errors.New("request declared a Content-Length of 5 but only wrote 2 bytes")
	}
	r.read = true

	return copy(p, "[]"), nil
}

// TestIssue8594ReviewServersTransportErrorShapes pins, through Traefik's own
// servers transport, the two error shapes ComputeStatusCode must accept for the same
// client fault: the "readfrom" *net.OpError produced on a cleartext connection to the
// backend, and the same error undressed on a TLS connection to the backend (no TCP
// ReadFrom fast path there). Any other shape a RoundTrip failure takes is a
// regression of these assertions.
func TestIssue8594ReviewServersTransportErrorShapes(t *testing.T) {
	for _, test := range []struct {
		desc      string
		backend   func(hits *atomic.Int32) *httptest.Server
		transport *dynamic.ServersTransport
		wantWrap  bool
	}{
		{
			desc:      "cleartext backend surfaces a readfrom OpError",
			backend:   func(hits *atomic.Int32) *httptest.Server { return httptest.NewServer(issue8594ReviewBackend(hits)) },
			transport: &dynamic.ServersTransport{},
			wantWrap:  true,
		},
		{
			desc: "TLS backend surfaces the mismatch error unwrapped",
			backend: func(hits *atomic.Int32) *httptest.Server {
				s := httptest.NewUnstartedServer(issue8594ReviewBackend(hits))
				s.StartTLS()
				return s
			},
			transport: &dynamic.ServersTransport{InsecureSkipVerify: true},
			wantWrap:  false,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			var hits atomic.Int32
			backend := test.backend(&hits)
			t.Cleanup(backend.Close)

			transportManager := service.NewTransportManager(nil)
			transportManager.Update(map[string]*dynamic.ServersTransport{"default@internal": test.transport})
			rt, err := transportManager.GetRoundTripper("default@internal")
			require.NoError(t, err)

			var captured []error
			for range 10 {
				req, err := http.NewRequest(http.MethodPut, backend.URL+"/badrequest", &shortBodyErrorReader{})
				require.NoError(t, err)
				req.ContentLength = 5

				resp, err := rt.RoundTrip(req)
				if err != nil {
					captured = append(captured, err)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}

			require.NotEmpty(t, captured, "the body copy must fail at least once")
			t.Logf("captured %d/%d round trips", len(captured), 10)

			for _, capturedErr := range captured {
				t.Logf("captured error type %T: %v", capturedErr, capturedErr)

				if test.wantWrap {
					var opErr *net.OpError
					require.ErrorAs(t, capturedErr, &opErr)
					assert.Equal(t, "readfrom", opErr.Op)
					assert.True(t, strings.HasPrefix(opErr.Err.Error(), issue8594ReviewPrefix), "wrapped error is %q", opErr.Err)
					continue
				}

				assert.True(t, strings.HasPrefix(capturedErr.Error(), issue8594ReviewPrefix), "captured=%v (%T)", capturedErr, capturedErr)
			}
		})
	}
}

// TestIssue8594ReviewHTTP2RSTStreamMidBody checks the behavior the PR defers: a
// client that resets the h2 stream mid-body. The reverse proxy never answers 502 for
// it: the transport reports the canceled request context (499), so the deferred
// claim "currently 502" is not the actual behavior.
func TestIssue8594ReviewHTTP2RSTStreamMidBody(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	backend := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		<-release
		rw.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(backend.Close)

	addr, capture := issue8594ReviewStart(t, backend.URL, false, &dynamic.ServersTransport{})

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	var hdr bytes.Buffer
	enc := hpack.NewEncoder(&hdr)
	for _, field := range [][2]string{
		{":method", "PUT"},
		{":scheme", "http"},
		{":authority", "localhost"},
		{":path", "/badrequest"},
		{"content-length", "5"},
	} {
		require.NoError(t, enc.WriteField(hpack.HeaderField{Name: field[0], Value: field[1]}))
	}

	_, err = conn.Write([]byte(http2.ClientPreface))
	require.NoError(t, err)
	fr := http2.NewFramer(conn, conn)
	require.NoError(t, fr.WriteSettings())

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	if f, rerr := fr.ReadFrame(); rerr == nil {
		if s, ok := f.(*http2.SettingsFrame); ok && !s.IsAck() {
			require.NoError(t, fr.WriteSettingsAck())
		}
	}
	require.NoError(t, conn.SetReadDeadline(time.Time{}))

	require.NoError(t, fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: hdr.Bytes(), EndHeaders: true}))
	require.NoError(t, fr.WriteData(1, false, []byte("[]")))
	require.NoError(t, fr.WriteRSTStream(1, http2.ErrCodeCancel))

	drain := make(chan struct{})
	go func() {
		defer close(drain)
		fr2 := http2.NewFramer(nil, conn)
		for {
			if _, err := fr2.ReadFrame(); err != nil {
				return
			}
		}
	}()

	var errs []error
	for range 100 {
		if errs = capture.get(); len(errs) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	conn.Close()
	<-drain
	require.NotEmpty(t, errs, "the body copy must fail after the stream reset")

	for _, err := range errs {
		t.Logf("captured error type %T: %v", err, err)
		assert.NotEqual(t, http.StatusBadGateway, httputil.ComputeStatusCode(err))
		assert.Equal(t, httputil.StatusClientClosedRequest, httputil.ComputeStatusCode(err), "captured=%v", err)
	}
}
