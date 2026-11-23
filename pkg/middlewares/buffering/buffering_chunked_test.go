package buffering_test

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/config/runtime"
	"github.com/traefik/traefik/v3/pkg/middlewares/buffering"
	"github.com/traefik/traefik/v3/pkg/proxy/httputil"
	"github.com/traefik/traefik/v3/pkg/server/service"
)

// These tests cover upstream issue 11568: with the buffering middleware
// enabled, a backend (Tomcat/Alfresco) answering "GET /alfresco" with a bare
// 302 and "Transfer-Encoding: chunked" (a single zero-length chunk) made the
// oxy response buffer fail with "no data ready" on Traefik v3.3.3 (oxy
// v2.0.0), and Traefik returned a 500 instead of forwarding the redirect.

const (
	alfrescoLocation = "/alfresco/"
	alfrescoTarget   = "alfresco@docker"
)

// TestServeHTTP_chunkedEmptyRedirect reproduces the setup of issue 11568: the
// buffering middleware enabled by setting maxRequestBodyBytes on the
// middleware, a backend (Alfresco on Tomcat) replying 302 with an empty
// chunked body, and a client requesting /alfresco without a trailing slash.
func TestServeHTTP_chunkedEmptyRedirect(t *testing.T) {
	t.Parallel()

	front, writes := newBufferedFrontend(t, rawTomcatBackend(t), dynamic.Buffering{MaxRequestBodyBytes: 1024})

	resp := getNoRedirect(t, front, "/alfresco")

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, alfrescoLocation, resp.Header.Get("Location"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Empty(t, body)

	// The forwarder must have finished the response without ever calling
	// Write: this is the exact precondition that made oxy v2.0.0 answer 500
	// ("mailgun/multibuf: no data ready") instead of forwarding the redirect.
	assert.Equal(t, http.StatusFound, writes.status())
	assert.Zero(t, writes.count())
}

// TestServeHTTP_chunkedEmptyRedirectWithRetry reproduces the standalone
// reproducers posted on issue 11568, where the oxy buffer is created with
// buffer.Retry(`IsNetworkError() && Attempts() < 2`).
func TestServeHTTP_chunkedEmptyRedirectWithRetry(t *testing.T) {
	t.Parallel()

	front, writes := newBufferedFrontend(t, rawTomcatBackend(t), dynamic.Buffering{
		MaxRequestBodyBytes: 1024,
		RetryExpression:     `IsNetworkError() && Attempts() < 2`,
	})

	resp := getNoRedirect(t, front, "/alfresco")

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, alfrescoLocation, resp.Header.Get("Location"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Empty(t, body)

	assert.Equal(t, http.StatusFound, writes.status())
	assert.Zero(t, writes.count())
}

// TestServeHTTP_chunkedEmptyRedirectDefaultConfig reproduces the plain
// "enabled the buffering middleware" setup reported from Kubernetes, without
// any size limit set.
func TestServeHTTP_chunkedEmptyRedirectDefaultConfig(t *testing.T) {
	t.Parallel()

	front, writes := newBufferedFrontend(t, rawTomcatBackend(t), dynamic.Buffering{})

	resp := getNoRedirect(t, front, "/alfresco")

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, alfrescoLocation, resp.Header.Get("Location"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Empty(t, body)

	assert.Equal(t, http.StatusFound, writes.status())
	assert.Zero(t, writes.count())
}

// TestServeHTTP_chunkedRedirectWithBody makes sure a redirect whose chunked
// body is not empty is still buffered and forwarded intact.
func TestServeHTTP_chunkedRedirectWithBody(t *testing.T) {
	t.Parallel()

	backend := startRawBackend(t, rawResponse(
		"HTTP/1.1 302",
		"Location: "+alfrescoLocation,
		"Transfer-Encoding: chunked",
		"",
		"11", "Moved Permanently",
		"0", "",
	))

	front, writes := newBufferedFrontend(t, backend, dynamic.Buffering{MaxRequestBodyBytes: 1024})

	resp := getNoRedirect(t, front, "/alfresco")

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, alfrescoLocation, resp.Header.Get("Location"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "Moved Permanently", string(body))

	assert.Positive(t, writes.count())
}

// TestServeHTTP_chunkedOKBody covers the request that worked in the issue
// report ("adding a trailing / works fine"): a normal 200 response streamed
// with chunked encoding must pass through the buffering middleware intact.
func TestServeHTTP_chunkedOKBody(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("hello, "))
		require.NoError(t, err)

		// Flushing before the body is complete forces a chunked response.
		w.(http.Flusher).Flush()

		_, err = w.Write([]byte("world"))
		require.NoError(t, err)
	}))
	t.Cleanup(backend.Close)

	front, writes := newBufferedFrontend(t, backend.URL, dynamic.Buffering{MaxRequestBodyBytes: 1024})

	resp := getNoRedirect(t, front, "/alfresco/")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "hello, world", string(body))

	assert.Positive(t, writes.count())
}

// TestServeHTTP_noContentResponses covers the responses oxy deliberately does
// not expect a body for (204), and the reporter's workaround of sending an
// explicit Content-Length: 0 on the redirect; both must be forwarded as is.
func TestServeHTTP_noContentResponses(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc           string
		response       []byte
		expectedStatus int
	}{
		{
			desc:           "204 no content",
			response:       rawResponse("HTTP/1.1 204 No Content", "Connection: close", ""),
			expectedStatus: http.StatusNoContent,
		},
		{
			desc: "302 with explicit Content-Length: 0",
			response: rawResponse(
				"HTTP/1.1 302",
				"Location: "+alfrescoLocation,
				"Content-Length: 0",
				"",
			),
			expectedStatus: http.StatusFound,
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			front, _ := newBufferedFrontend(t, startRawBackend(t, test.response), dynamic.Buffering{MaxRequestBodyBytes: 1024})

			resp := getNoRedirect(t, front, "/alfresco")

			assert.Equal(t, test.expectedStatus, resp.StatusCode)
		})
	}
}

// TestServeHTTP_backendErrorPassthrough makes sure the buffering middleware
// does not swallow regular error responses from the backend.
func TestServeHTTP_backendErrorPassthrough(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("backend exploded"))
		require.NoError(t, err)
	}))
	t.Cleanup(backend.Close)

	front, _ := newBufferedFrontend(t, backend.URL, dynamic.Buffering{MaxRequestBodyBytes: 1024})

	resp := getNoRedirect(t, front, "/alfresco")

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "backend exploded", string(body))
}

// newBufferedFrontend wires the components the way a Traefik configuration
// with a single buffering middleware and a single-server load balancer wires
// them: an entry point (real HTTP server) serving the buffering middleware on
// top of the HTTP service handler built by the service manager (weighted
// round-robin, reverse proxy, default servers transport). It also returns the
// write counter placed between the buffering middleware and the service
// handler, which records what the forwarder writes for the buffered response.
func newBufferedFrontend(t *testing.T, backendURL string, config dynamic.Buffering) (*httptest.Server, *countingWriter) {
	t.Helper()

	transportManager := service.NewTransportManager(nil)
	transportManager.Update(map[string]*dynamic.ServersTransport{"default@internal": {}})

	serviceManager := service.NewManager(
		map[string]*runtime.ServiceInfo{
			alfrescoTarget: {
				Service: &dynamic.Service{
					LoadBalancer: &dynamic.ServersLoadBalancer{
						Strategy: dynamic.BalancerStrategyWRR,
						Servers:  []dynamic.Server{{URL: backendURL}},
					},
				},
			},
		},
		nil,
		nil,
		transportManager,
		httputil.NewProxyBuilder(transportManager, nil),
	)

	serviceHandler, err := serviceManager.BuildHTTP(t.Context(), alfrescoTarget)
	require.NoError(t, err)

	writes := &countingWriter{next: serviceHandler}

	handler, err := buffering.New(t.Context(), writes, config, "limit@docker")
	require.NoError(t, err)

	front := httptest.NewServer(handler)
	t.Cleanup(front.Close)

	return front, writes
}

// countingWriter counts the Write calls the service handler performs on the
// response writer handed down by the buffering middleware.
type countingWriter struct {
	next http.Handler

	mu     sync.Mutex
	writes int
	code   int
}

func (c *countingWriter) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	c.next.ServeHTTP(&countingResponseWriter{ResponseWriter: rw, parent: c}, req)
}

func (c *countingWriter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.writes
}

func (c *countingWriter) status() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.code
}

type countingResponseWriter struct {
	http.ResponseWriter

	parent *countingWriter
}

func (c *countingResponseWriter) Write(b []byte) (int, error) {
	c.parent.mu.Lock()
	c.parent.writes++
	c.parent.mu.Unlock()

	return c.ResponseWriter.Write(b)
}

func (c *countingResponseWriter) WriteHeader(status int) {
	c.parent.mu.Lock()
	c.parent.code = status
	c.parent.mu.Unlock()

	c.ResponseWriter.WriteHeader(status)
}

// Unwrap keeps the http.ResponseController chain intact, so deadlines,
// flushing and hijacking reach the buffering middleware's writer below.
func (c *countingResponseWriter) Unwrap() http.ResponseWriter {
	return c.ResponseWriter
}

// rawTomcatBackend serves the exact response the application in issue 11568
// sent for "GET /alfresco".
func rawTomcatBackend(t *testing.T) string {
	t.Helper()

	raw, err := os.ReadFile("testdata/tomcat-302-chunked-empty.http")
	require.NoError(t, err)

	return startRawBackend(t, raw)
}

// startRawBackend mimics an application server that answers every request
// with the given canned response bytes, bypassing any Go server framing
// heuristics, so the wire bytes are exactly the ones under test.
func startRawBackend(t *testing.T, response []byte) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go serveCanned(conn, response)
		}
	}()

	return "http://" + listener.Addr().String()
}

func serveCanned(conn net.Conn, response []byte) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		// Drain the request: the tests only send bodyless GETs.
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}

			if line == "\r\n" || line == "\n" {
				break
			}
		}

		if _, err := conn.Write(response); err != nil {
			return
		}
	}
}

// rawResponse renders an HTTP response from its logical lines, one per
// element, with CRLF terminators.
func rawResponse(lines ...string) []byte {
	return []byte(strings.Join(lines, "\r\n") + "\r\n")
}

func getNoRedirect(t *testing.T, front *httptest.Server, path string) *http.Response {
	t.Helper()

	client := &http.Client{
		Transport:     &http.Transport{},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	resp, err := client.Get(front.URL + path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}
