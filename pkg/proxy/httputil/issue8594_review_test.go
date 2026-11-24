package httputil

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIssue8594ReviewComputeStatusCode pins the classification of the unexported
// HTTP/2 short-body error: both shapes the servers transport delivers (wrapped in a
// "readfrom" *net.OpError for cleartext backends, unwrapped for TLS backends) must
// yield 400, while unrelated network errors keep their previous status.
func TestIssue8594ReviewComputeStatusCode(t *testing.T) {
	mismatch := errors.New("request declared a Content-Length of 5 but only wrote 2 bytes")
	unrelated := errors.New("connection reset by peer")

	testCases := []struct {
		desc     string
		err      error
		expected int
	}{
		{
			desc:     "mismatch error unwrapped (TLS backend path)",
			err:      mismatch,
			expected: http.StatusBadRequest,
		},
		{
			desc:     "mismatch error wrapped in readfrom OpError (cleartext backend path)",
			err:      &net.OpError{Op: "readfrom", Net: "tcp", Err: mismatch},
			expected: http.StatusBadRequest,
		},
		{
			desc:     "readfrom OpError with an unrelated error stays bad gateway",
			err:      &net.OpError{Op: "readfrom", Net: "tcp", Err: unrelated},
			expected: http.StatusBadGateway,
		},
		{
			desc:     "mismatch text under a dial OpError is not a client fault",
			err:      &net.OpError{Op: "dial", Net: "tcp", Err: mismatch},
			expected: http.StatusBadGateway,
		},
		{
			desc:     "timeout stays gateway timeout",
			err:      &fakeTimeoutError{},
			expected: http.StatusGatewayTimeout,
		},
		{
			desc:     "EOF stays bad gateway",
			err:      io.EOF,
			expected: http.StatusBadGateway,
		},
		{
			desc:     "canceled request stays 499",
			err:      context.Canceled,
			expected: StatusClientClosedRequest,
		},
		{
			desc:     "unknown error stays internal server error",
			err:      errors.New("boom"),
			expected: http.StatusInternalServerError,
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.expected, ComputeStatusCode(test.err))
		})
	}
}

type fakeTimeoutError struct{}

func (fakeTimeoutError) Error() string { return "i/o timeout" }
func (fakeTimeoutError) Timeout() bool { return true }

// Temporary is required by net.Error, which still demands it in Go 1.27.
func (fakeTimeoutError) Temporary() bool { return true }
