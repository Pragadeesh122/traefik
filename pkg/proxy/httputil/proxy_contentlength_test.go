package httputil

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/testhelpers"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// startH2CProxy wires the real reverse proxy (built through the ProxyBuilder, i.e. the same
// httputil.ReverseProxy + ErrorHandler a user's routing configuration produces) behind an
// HTTP/1.1 + prior-knowledge HTTP/2 server, the way an entrypoint fronts it.
func startH2CProxy(t *testing.T, backendURL string) *httptest.Server {
	t.Helper()

	transportManager := &transportManagerMock{
		roundTrippers: map[string]http.RoundTripper{"default": &http.Transport{}},
	}
	handler, err := NewProxyBuilder(transportManager, nil).Build("default", testhelpers.MustParseURL(backendURL), true, false, -1)
	require.NoError(t, err)

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	proxy := httptest.NewUnstartedServer(handler)
	proxy.Config.Protocols = protocols
	proxy.Start()
	t.Cleanup(proxy.Close)

	return proxy
}

// doH2CRequest talks to addr in prior-knowledge HTTP/2 using raw frames, so that the request it
// sends can be deliberately malformed: the HEADERS declare content-length declaredCL while the
// single DATA frame carries only len(body) bytes and ends the stream (END_STREAM).
func doH2CRequest(t *testing.T, addr, method, path, body string, declaredCL int) (int, string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	_, err = conn.Write([]byte(http2.ClientPreface))
	require.NoError(t, err)

	fr := http2.NewFramer(conn, conn)
	require.NoError(t, fr.WriteSettings())

	var hdr bytes.Buffer
	enc := hpack.NewEncoder(&hdr)
	require.NoError(t, enc.WriteField(hpack.HeaderField{Name: ":method", Value: method}))
	require.NoError(t, enc.WriteField(hpack.HeaderField{Name: ":scheme", Value: "http"}))
	require.NoError(t, enc.WriteField(hpack.HeaderField{Name: ":path", Value: path}))
	require.NoError(t, enc.WriteField(hpack.HeaderField{Name: ":authority", Value: addr}))
	require.NoError(t, enc.WriteField(hpack.HeaderField{Name: "content-length", Value: fmt.Sprint(declaredCL)}))
	require.NoError(t, fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: hdr.Bytes(), EndHeaders: true}))
	require.NoError(t, fr.WriteData(1, true, []byte(body)))

	status := 0
	var respBody bytes.Buffer
	dec := hpack.NewDecoder(4096, nil)
	for {
		f, rerr := fr.ReadFrame()
		require.NoError(t, rerr, "reading response frame (status=%d)", status)

		switch ff := f.(type) {
		case *http2.SettingsFrame:
			if !ff.IsAck() {
				_ = fr.WriteSettings()
			}
		case *http2.PingFrame:
			_ = fr.WritePing(true, ff.Data)
		case *http2.HeadersFrame:
			if ff.StreamID != 1 {
				continue
			}
			fields, derr := dec.DecodeFull(ff.HeaderBlockFragment())
			require.NoError(t, derr)
			for _, hf := range fields {
				if hf.Name == ":status" {
					_, _ = fmt.Sscanf(hf.Value, "%d", &status)
				}
			}
			if ff.StreamEnded() {
				return status, respBody.String()
			}
		case *http2.DataFrame:
			if ff.StreamID == 1 {
				respBody.Write(ff.Data())
				if ff.StreamEnded() {
					return status, respBody.String()
				}
			}
		}
	}
}

// TestReverseProxyHTTP2ContentLengthMismatch reproduces issue 8594: an HTTP/2 request that declares
// a Content-Length larger than the body it actually sends. The backend is reached and answers 400,
// yet Traefik hides it behind a 502 Bad Gateway, because forwarding the (truncated) request body
// fails. A faulty request is a client error, so 400 is what the client should observe.
func TestReverseProxyHTTP2ContentLengthMismatch(t *testing.T) {
	var backendCalled atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		backendCalled.Store(true)
		rw.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(rw, "oops something went wrong")
	}))
	t.Cleanup(backend.Close)

	proxy := startH2CProxy(t, backend.URL)

	// Declares content-length: 5 but only sends the 2 bytes "[]", then ends the stream.
	status, _ := doH2CRequest(t, proxy.Listener.Addr().String(), http.MethodPut, "/badrequest", "[]", 5)

	// Precondition: the request really reached the backend, which returned 400.
	require.True(t, backendCalled.Load(), "backend was never contacted")
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestReverseProxyHTTP2ContentLengthCorrect is the control for the reproduction above: when the
// declared Content-Length matches the body, the backend response is forwarded verbatim and the
// reverse proxy does not turn it into a 502.
func TestReverseProxyHTTP2ContentLengthCorrect(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(rw, "oops something went wrong")
	}))
	t.Cleanup(backend.Close)

	proxy := startH2CProxy(t, backend.URL)

	status, body := doH2CRequest(t, proxy.Listener.Addr().String(), http.MethodPut, "/badrequest", "[]", 2)

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "oops something went wrong", body)
}

// TestReverseProxyHTTP2BodyForwardedIntact guards the ordinary forwarding path a fix must not
// break: a well-formed HTTP/2 request body reaches the backend byte-for-byte.
func TestReverseProxyHTTP2BodyForwardedIntact(t *testing.T) {
	const payload = "the quick brown fox jumps over the lazy dog"
	var got atomic.Value
	backend := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		got.Store(string(b))
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write(b)
	}))
	t.Cleanup(backend.Close)

	proxy := startH2CProxy(t, backend.URL)

	status, body := doH2CRequest(t, proxy.Listener.Addr().String(), http.MethodPut, "/echo", payload, len(payload))

	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, payload, body)
	require.NotNil(t, got.Load())
	assert.Equal(t, payload, got.Load().(string))
}

// TestReverseProxyHTTP1ContentLengthMismatchNoBadGateway pins the asymmetry the reporter relied on:
// the very same faulty request over HTTP/1.1 must not be answered with a 502. The client declares
// Content-Length: 5, sends 2 bytes and keeps the connection open, so Traefik is still waiting for
// the rest of the body and must stay silent rather than report a gateway error.
func TestReverseProxyHTTP1ContentLengthMismatchNoBadGateway(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(rw, "oops something went wrong")
	}))
	t.Cleanup(backend.Close)

	proxy := startH2CProxy(t, backend.URL)

	conn, err := net.DialTimeout("tcp", proxy.Listener.Addr().String(), 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// No END of body: the connection is left open with 3 promised bytes missing.
	_, err = conn.Write([]byte("PUT /badrequest HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\n[]"))
	require.NoError(t, err)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	line, rerr := bufio.NewReader(conn).ReadString('\n')
	if rerr == nil {
		assert.NotContains(t, line, "502", "HTTP/1.1 must not answer the truncated body with a Bad Gateway")
	}
}

// TestReverseProxyUnreachableBackendBadGateway makes sure a fix for the content-length case keeps
// the legitimate 502: when the backend cannot be reached at all, Traefik must still report a Bad
// Gateway for a well-formed request.
func TestReverseProxyUnreachableBackendBadGateway(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {}))
	backendURL := backend.URL
	backend.Close()

	proxy := startH2CProxy(t, backendURL)

	status, _ := doH2CRequest(t, proxy.Listener.Addr().String(), http.MethodPut, "/badrequest", "[]", 2)

	assert.Equal(t, http.StatusBadGateway, status)
}
