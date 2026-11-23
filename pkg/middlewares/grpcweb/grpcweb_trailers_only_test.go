package grpcweb_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/net/http2/hpack"

	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/middlewares/grpcweb"
	"github.com/traefik/traefik/v3/pkg/proxy/httputil"
	"github.com/traefik/traefik/v3/pkg/server/service"
)

// emptyGRPCTrailerFrame is the gRPC-Web trailer frame the middleware appends when it
// has nothing to serialise: MSB=1 marker + a zero-length 32-bit payload length.
var emptyGRPCTrailerFrame = []byte{1 << 7, 0, 0, 0, 0}

// buildGRPWebStack wires the real grpcWeb middleware in front of the real Traefik
// reverse proxy (the httputil builder, the default in cmd/traefik), pointed at target
// over h2c. It is returned behind an HTTP/1.1 test server, matching the reported
// topology: client speaks HTTP/1.1 to Traefik, Traefik speaks h2c to the backend.
func buildGRPWebStack(t *testing.T, target *url.URL) *httptest.Server {
	t.Helper()

	transportManager := service.NewTransportManager(nil)
	transportManager.Update(map[string]*dynamic.ServersTransport{"default@internal": {}})

	proxyBuilder := httputil.NewProxyBuilder(transportManager, nil)
	proxy, err := proxyBuilder.Build("default@internal", target, true, false, 0)
	require.NoError(t, err)

	cfg := dynamic.GrpcWeb{AllowOrigins: []string{"*"}}
	handler := grpcweb.New(context.Background(), proxy, cfg, "grpc-web")

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return srv
}

// grpcFrame wraps payload into a gRPC length-prefixed message frame.
func grpcFrame(payload []byte) []byte {
	frame := make([]byte, 5+len(payload))
	frame[0] = 0 // uncompressed
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)))
	copy(frame[5:], payload)
	return frame
}

func readAll(t *testing.T, r io.Reader) []byte {
	t.Helper()

	b, err := io.ReadAll(r)
	require.NoError(t, err)

	return b
}

// postGRPWeb issues a real gRPC-Web POST the way a browser connect-es client would.
func postGRPWeb(t *testing.T, srv *httptest.Server, path, contentType string, body []byte) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("content-type", contentType)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)

	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

// startHTTP2Backend runs an h2c (HTTP/2 cleartext) server backed by net/http, used for
// the response shapes that legitimately carry a body.
func startHTTP2Backend(t *testing.T, handlers map[string]http.HandlerFunc) *url.URL {
	t.Helper()

	mux := http.NewServeMux()
	for path, h := range handlers {
		mux.HandleFunc(path, h)
	}

	h2srv := &http2.Server{}
	srv := &http.Server{
		Handler:           h2c.NewHandler(mux, h2srv),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = srv.Serve(ln)
	}()
	t.Cleanup(func() { _ = srv.Close() })

	return &url.URL{Scheme: "h2c", Host: ln.Addr().String()}
}

// startRawTrailersOnlyBackend runs a minimal h2c server (a Go port of the reporter's
// node:http2 reproducer) that answers every stream with a single HEADERS frame carrying
// the gRPC status and END_STREAM: a genuine trailers-only gRPC response, deliberately
// without a content-length header (which the Go net/http server would synthesise) so the
// appended frame reaches the client instead of being dropped by the HTTP/1.1 server.
func startRawTrailersOnlyBackend(t *testing.T, headers [][2]string) *url.URL {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveRawTrailersOnly(conn, headers)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })

	return &url.URL{Scheme: "h2c", Host: ln.Addr().String()}
}

// serveRawTrailersOnly performs just enough of the HTTP/2 handshake to read one request
// stream and answer it with the supplied HEADERS frame (end of stream, no DATA, no trailers).
func serveRawTrailersOnly(conn net.Conn, headers [][2]string) {
	defer conn.Close()

	br := bufio.NewReader(conn)
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(br, preface); err != nil {
		return
	}
	if string(preface) != http2.ClientPreface {
		return
	}

	f := http2.NewFramer(conn, br)
	_ = f.WriteSettings()

	var streamID uint32
	responded := false
	respond := func() {
		if responded {
			return
		}
		responded = true
		writeRawHeaders(f, streamID, headers)
		// Keep the connection open so the client can read the response we just
		// queued, then drain whatever the client sends until it closes; closing
		// eagerly would reset the connection before the client has read the frame.
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	}

	for {
		frame, err := f.ReadFrame()
		if err != nil {
			return
		}

		switch fr := frame.(type) {
		case *http2.SettingsFrame:
			if !fr.IsAck() {
				_ = f.WriteSettingsAck()
			}

		case *http2.HeadersFrame:
			streamID = fr.StreamID
			if fr.Flags&http2.FlagHeadersEndStream != 0 {
				respond()
			}

		case *http2.DataFrame:
			if fr.Flags&http2.FlagDataEndStream != 0 {
				respond()
			}
		}
	}
}

func writeRawHeaders(f *http2.Framer, streamID uint32, headers [][2]string) {
	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, kv := range headers {
		_ = enc.WriteField(hpack.HeaderField{Name: kv[0], Value: kv[1]})
	}
	_ = f.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      streamID,
		BlockFragment: block.Bytes(),
		EndStream:     true,
		EndHeaders:    true,
	})
}

// trailersOnlyHeaders is the HEADERS set the reporter's backend emits: the gRPC status
// carried entirely in the initial HEADERS frame.
var trailersOnlyHeaders = [][2]string{
	{":status", "200"},
	{"content-type", "application/grpc"},
	{"grpc-status", "3"},
	{"grpc-message", "trailers-only response"},
}

// normalGRPCHandler answers like a successful gRPC call: a message in the body, then the
// gRPC status carried in HTTP/2 trailers.
func normalGRPCHandler(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)

	h := w.Header()
	h.Set("content-type", "application/grpc")
	h.Set("Trailer", "Grpc-Status, Grpc-Message")
	w.WriteHeader(http.StatusOK)

	_, _ = w.Write(grpcFrame([]byte("hello from backend")))
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	h.Set("Grpc-Status", "0")
	h.Set("Grpc-Message", "ok")
}

// plainHandler answers a non gRPC request with a plain body, used for the passthrough check.
func plainHandler(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)

	w.Header().Set("content-type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("plain body"))
}

// TestTrailersOnlyBinaryMustNotAppendEmptyTrailerFrame reproduces
// https://github.com/traefik/traefik/issues/13842 for the binary gRPC-Web content type:
// a trailers-only upstream response must be relayed as headers only, with no body,
// yet the middleware appends the empty frame 80 00 00 00 00.
func TestTrailersOnlyBinaryMustNotAppendEmptyTrailerFrame(t *testing.T) {
	target := startRawTrailersOnlyBackend(t, trailersOnlyHeaders)
	srv := buildGRPWebStack(t, target)

	resp := postGRPWeb(t, srv, "/repro.v1.Service/Method", "application/grpc-web+proto", grpcFrame(nil))
	body := readAll(t, resp.Body)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "3", resp.Header.Get("Grpc-Status"))

	assert.Empty(t, body, "trailers-only response must have no body, but the middleware appended the empty trailer frame %x", body)
}

// TestTrailersOnlyTextMustNotAppendEmptyTrailerFrame is the same reproduction for the
// base64 wire format (application/grpc-web-text), the variant covered by the upstream
// dependency pull request traefik/grpc-web#4.
func TestTrailersOnlyTextMustNotAppendEmptyTrailerFrame(t *testing.T) {
	target := startRawTrailersOnlyBackend(t, trailersOnlyHeaders)
	srv := buildGRPWebStack(t, target)

	reqBody := base64.StdEncoding.EncodeToString(grpcFrame(nil))
	resp := postGRPWeb(t, srv, "/repro.v1.Service/Method", "application/grpc-web-text", []byte(reqBody))
	raw := readAll(t, resp.Body)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "3", resp.Header.Get("Grpc-Status"))

	decoded, err := base64.StdEncoding.DecodeString(string(raw))
	require.NoError(t, err, "grpc-web-text body must be valid base64, got %q", string(raw))

	assert.Empty(t, decoded, "trailers-only response must have no body, but the middleware appended the empty trailer frame %x", decoded)
}

// TestTrailersOnlyKeepsStatusHeaders is a must-not-change check: whatever the fix does to
// the body, the gRPC status must stay exposed as response headers (exactly what
// PROTOCOL-WEB prescribes for a trailers-only response).
func TestTrailersOnlyKeepsStatusHeaders(t *testing.T) {
	target := startRawTrailersOnlyBackend(t, trailersOnlyHeaders)
	srv := buildGRPWebStack(t, target)

	resp := postGRPWeb(t, srv, "/repro.v1.Service/Method", "application/grpc-web+proto", grpcFrame(nil))
	_ = readAll(t, resp.Body)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "3", resp.Header.Get("Grpc-Status"))
	assert.Equal(t, "trailers-only response", resp.Header.Get("Grpc-Message"))
	assert.Equal(t, "application/grpc-web", resp.Header.Get("Content-Type"))
}

// TestNormalResponseStillEmitsTrailerFrame is a must-not-change check: a real gRPC
// response that carries a message and its status in HTTP/2 trailers must still get a
// non-empty gRPC-Web trailer frame, so a fix must not suppress frames that have content.
func TestNormalResponseStillEmitsTrailerFrame(t *testing.T) {
	target := startHTTP2Backend(t, map[string]http.HandlerFunc{
		"/repro.v1.Service/Method": normalGRPCHandler,
	})
	srv := buildGRPWebStack(t, target)

	resp := postGRPWeb(t, srv, "/repro.v1.Service/Method", "application/grpc-web+proto", grpcFrame([]byte("ping")))
	body := readAll(t, resp.Body)

	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Contains(t, string(body), "hello from backend", "response message must be relayed")
	assert.NotEqual(t, emptyGRPCTrailerFrame, body, "a response with real trailers must not collapse to the empty trailer frame")
	assert.Contains(t, string(body), "grpc-status", "the trailer frame must carry the gRPC status")
}

// TestNonGRPWebRequestPassesThrough is a must-not-change check: a request that is not a
// gRPC-Web request must reach the backend untouched, with no gRPC-Web framing applied.
func TestNonGRPWebRequestPassesThrough(t *testing.T) {
	target := startHTTP2Backend(t, map[string]http.HandlerFunc{
		"/plain": plainHandler,
	})
	srv := buildGRPWebStack(t, target)

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/plain", nil)
	require.NoError(t, err)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body := readAll(t, resp.Body)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "plain body", string(body))
}
