package libgm

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	"go.mau.fi/util/pblite"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func ackTestClient(t *testing.T, handle func(*http.Request, *gmproto.AckMessageRequest) (*http.Response, error)) *Client {
	t.Helper()
	settings := exhttp.SensibleClientSettings
	settings.TransportOverride = func(exhttp.ClientSettings) http.RoundTripper {
		return methodRoundTripper(func(req *http.Request) (*http.Response, error) {
			if !strings.HasSuffix(req.URL.Path, "/AckMessages") {
				return nil, fmt.Errorf("unexpected test request: %s", req.URL.Path)
			}
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			var payload gmproto.AckMessageRequest
			if err := pblite.Unmarshal(body, &payload); err != nil {
				return nil, err
			}
			return handle(req, &payload)
		})
	}
	cli := NewClient(NewAuthData(), nil, zerolog.Nop(), settings)
	t.Cleanup(cli.Disconnect)
	return cli
}

func ackTestResponse(req *http.Request, status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": {ContentTypePBLite}},
		Body:       io.NopCloser(strings.NewReader("[]")),
		Request:    req,
	}
}

func currentAckRun(s *SessionHandler) *ackInterval {
	s.ackRunLock.Lock()
	defer s.ackRunLock.Unlock()
	return s.ackRun
}

func waitAckSignal(t *testing.T, done <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestAckIntervalRetriesOnlyWhileActive(t *testing.T) {
	var requests atomic.Int32
	var succeed atomic.Bool
	called := make(chan struct{}, 20)
	cli := ackTestClient(t, func(req *http.Request, payload *gmproto.AckMessageRequest) (*http.Response, error) {
		requests.Add(1)
		if len(payload.Acks) != 1 || payload.Acks[0].RequestID != "synthetic-ack" {
			t.Errorf("retry did not preserve the queued ack: %v", payload.Acks)
		}
		called <- struct{}{}
		status := http.StatusUnauthorized
		if succeed.Load() {
			status = http.StatusOK
		}
		return ackTestResponse(req, status), nil
	})
	s := cli.sessionHandler
	s.startAckInterval(t.Context())
	first := currentAckRun(s)
	if first == nil {
		t.Fatal("interval did not start")
	}
	s.startAckInterval(t.Context())
	if currentAckRun(s) != first {
		t.Fatal("starting an active interval created a duplicate worker")
	}
	first.ticker.Reset(5 * time.Millisecond)
	s.queueMessageAck("synthetic-ack")
	waitAckSignal(t, called, "first ack request")
	waitAckSignal(t, called, "failed ack retry")
	cli.Disconnect()
	waitAckSignal(t, first.done, "retired worker")
	before := requests.Load()
	s.sendAckRequest() // A delayed post-connect callback must not send either.
	time.Sleep(25 * time.Millisecond)
	if got := requests.Load(); got != before {
		t.Fatalf("retired interval made %d more requests", got-before)
	}
	cli.Disconnect() // Repeated disconnect remains harmless.
	succeed.Store(true)
	s.startAckInterval(t.Context())
	if next := currentAckRun(s); next == nil || next == first {
		t.Fatal("interval did not restart with a new lifecycle")
	}
	s.sendAckRequestForRun(first)
	if requests.Load() != before {
		t.Fatal("retired worker drained the new interval's queue")
	}
	s.sendAckRequest()
	if requests.Load() != before+1 {
		t.Fatal("restarted interval did not retry its retained ack")
	}
	s.ackMapLock.Lock()
	defer s.ackMapLock.Unlock()
	if len(s.ackMap) != 0 {
		t.Fatalf("successful retry left %d acks queued", len(s.ackMap))
	}
}

func TestAckDisconnectCancelsAndJoinsInflightRequests(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct_flush_%t", direct), func(t *testing.T) {
			entered := make(chan struct{})
			canceled := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			cli := ackTestClient(t, func(req *http.Request, _ *gmproto.AckMessageRequest) (*http.Response, error) {
				close(entered)
				<-req.Context().Done()
				close(canceled)
				<-release // Prove Disconnect joins the request, not only cancels it.
				return nil, req.Context().Err()
			})
			s := cli.sessionHandler
			s.startAckInterval(t.Context())
			run := currentAckRun(s)
			s.queueMessageAck("inflight-ack")
			flushDone := make(chan struct{})
			if direct {
				go func() {
					s.sendAckRequest()
					close(flushDone)
				}()
			} else {
				run.ticker.Reset(time.Millisecond)
			}
			waitAckSignal(t, entered, "inflight ack")
			stopped := make(chan struct{})
			go func() {
				cli.Disconnect()
				close(stopped)
			}()
			waitAckSignal(t, canceled, "HTTP cancellation")
			select {
			case <-stopped:
				t.Fatal("Disconnect returned while an ack request was still in flight")
			default:
			}
			release <- struct{}{}
			waitAckSignal(t, stopped, "Disconnect completion")
			waitAckSignal(t, run.done, "ack worker completion")
			if direct {
				waitAckSignal(t, flushDone, "direct flush completion")
			}
			if currentAckRun(s) != nil {
				t.Fatal("Disconnect retained an active interval")
			}
		})
	}
}

func TestAckContextCancellationAllowsRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	var requests atomic.Int32
	cli := ackTestClient(t, func(req *http.Request, _ *gmproto.AckMessageRequest) (*http.Response, error) {
		if requests.Add(1) == 1 {
			close(entered)
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		return ackTestResponse(req, http.StatusOK), nil
	})
	s := cli.sessionHandler
	s.startAckInterval(ctx)
	first := currentAckRun(s)
	first.ticker.Reset(time.Millisecond)
	s.queueMessageAck("canceled-ack")
	waitAckSignal(t, entered, "ack request")
	cancel()
	// Restart immediately, while the canceled worker may still be unwinding.
	s.startAckInterval(t.Context())
	waitAckSignal(t, first.done, "context-canceled interval")
	if currentAckRun(s) == first {
		t.Fatal("canceled interval was reused")
	}
	s.sendAckRequest()
	if requests.Load() != 2 {
		t.Fatalf("expected one canceled request and one retry, got %d", requests.Load())
	}
}

func TestAckIntervalAlreadyCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var requests atomic.Int32
	cli := ackTestClient(t, func(req *http.Request, _ *gmproto.AckMessageRequest) (*http.Response, error) {
		requests.Add(1)
		return ackTestResponse(req, http.StatusOK), nil
	})
	s := cli.sessionHandler
	s.startAckInterval(ctx)
	s.queueMessageAck("unsent-ack")
	s.sendAckRequest()
	if currentAckRun(s) != nil || requests.Load() != 0 {
		t.Fatal("already-canceled context started ack work")
	}
}

func TestAckRetryQueueRemainsBounded(t *testing.T) {
	var cli *Client
	cli = ackTestClient(t, func(req *http.Request, _ *gmproto.AckMessageRequest) (*http.Response, error) {
		for i := range maxQueuedAcks {
			cli.sessionHandler.queueMessageAck(fmt.Sprintf("new-ack-%d", i))
		}
		return ackTestResponse(req, http.StatusUnauthorized), nil
	})
	s := cli.sessionHandler
	s.startAckInterval(t.Context())
	s.queueMessageAck("failed-ack")
	s.sendAckRequest()
	s.ackMapLock.Lock()
	defer s.ackMapLock.Unlock()
	if got := len(s.ackMap); got != maxQueuedAcks {
		t.Fatalf("failed retry changed queue bound: %d, want %d", got, maxQueuedAcks)
	}
}
