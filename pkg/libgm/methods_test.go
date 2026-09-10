package libgm

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	"go.mau.fi/util/pblite"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

type methodRoundTripper func(*http.Request) (*http.Response, error)

func (f methodRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func methodTestClient(t *testing.T, handle func(*Client, *gmproto.OutgoingRPCMessage, *gmproto.OutgoingRPCData)) *Client {
	t.Helper()
	var cli *Client
	settings := exhttp.SensibleClientSettings
	settings.TransportOverride = func(exhttp.ClientSettings) http.RoundTripper {
		return methodRoundTripper(func(req *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			var envelope gmproto.OutgoingRPCMessage
			if err := pblite.Unmarshal(body, &envelope); err != nil {
				return nil, err
			}
			var data gmproto.OutgoingRPCData
			if err := proto.Unmarshal(envelope.GetData().GetMessageData(), &data); err != nil {
				return nil, err
			}
			handle(cli, &envelope, &data)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {ContentTypePBLite}},
				Body:       io.NopCloser(strings.NewReader("[]")),
				Request:    req,
			}, nil
		})
	}
	cli = NewClient(NewAuthData(), nil, zerolog.Nop(), settings)
	t.Cleanup(cli.Disconnect)
	return cli
}

func TestListConversationsPreservesPaginationOnWire(t *testing.T) {
	cursor := &gmproto.Cursor{}
	// Unknown fields let the test carry an opaque future cursor through the
	// entire request serialization without assuming its internal schema.
	cursor.ProtoReflect().SetUnknown([]byte{0x08, 0x2a})
	requests := 0
	cli := methodTestClient(t, func(cli *Client, envelope *gmproto.OutgoingRPCMessage, data *gmproto.OutgoingRPCData) {
		requests++
		if data.GetAction() != gmproto.ActionType_LIST_CONVERSATIONS {
			t.Fatalf("action = %v", data.GetAction())
		}
		plain, err := cli.AuthData.RequestCrypto.Decrypt(data.GetEncryptedProtoData())
		if err != nil {
			t.Fatal(err)
		}
		var request gmproto.ListConversationsRequest
		if err := proto.Unmarshal(plain, &request); err != nil {
			t.Fatal(err)
		}
		if request.GetCount() != 100 || request.GetFolder() != gmproto.ListConversationsRequest_INBOX {
			t.Fatalf("page parameters changed: %v", &request)
		}
		wantType := gmproto.MessageType_BUGLE_ANNOTATION
		if requests == 1 {
			if request.GetCursor() != nil {
				t.Fatal("first page unexpectedly has a cursor")
			}
		} else {
			wantType = gmproto.MessageType_BUGLE_MESSAGE
			if !proto.Equal(request.GetCursor(), cursor) {
				t.Fatal("continuation cursor was lost or changed")
			}
		}
		if envelope.GetData().GetMessageTypeData().GetMessageType() != wantType {
			t.Fatal("first-page annotation or continuation message type changed")
		}
		cli.sessionHandler.responseWaitersLock.Lock()
		ch := cli.sessionHandler.responseWaiters[data.GetRequestID()]
		delete(cli.sessionHandler.responseWaiters, data.GetRequestID())
		cli.sessionHandler.responseWaitersLock.Unlock()
		ch <- &IncomingRPCMessage{DecryptedMessage: &gmproto.ListConversationsResponse{}}
	})
	ctx := t.Context()
	if _, err := cli.ListConversations(ctx, 100, gmproto.ListConversationsRequest_INBOX); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.ListConversationsWithCursor(ctx, 100, gmproto.ListConversationsRequest_INBOX, cursor); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestNotifyDittoActivityCancellationReleasesWaiter(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cli := methodTestClient(t, func(_ *Client, _ *gmproto.OutgoingRPCMessage, data *gmproto.OutgoingRPCData) {
		if data.GetAction() != gmproto.ActionType_NOTIFY_DITTO_ACTIVITY {
			t.Fatalf("action = %v", data.GetAction())
		}
		cancel()
	})
	if err := cli.NotifyDittoActivity(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation", err)
	}
	cli.sessionHandler.responseWaitersLock.Lock()
	defer cli.sessionHandler.responseWaitersLock.Unlock()
	if len(cli.sessionHandler.responseWaiters) != 0 {
		t.Fatal("canceled probe left a response waiter behind")
	}
}
