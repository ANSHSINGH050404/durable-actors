package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	modal "github.com/modal-labs/modal-client/go"
	pb "github.com/modal-labs/modal-client/go/proto/modal_proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestSDKInitializesWithTheRustProvidersSanitizedEnvironment(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("MODAL_CONFIG_PATH", "")
	t.Setenv("MODAL_TOKEN_ID", "test-token")
	t.Setenv("MODAL_TOKEN_SECRET", "test-secret")
	api, closeClient, err := newModalAPI()
	if err != nil {
		t.Fatal(err)
	}
	defer closeClient()
	if api == nil {
		t.Fatal("no SDK client")
	}
}

func TestResolveCachesAppAndEachImage(t *testing.T) {
	api, calls := testSDKAPI(t)
	for _, id := range []string{"im-one", "im-two", "im-one", "im-two"} {
		app, image, err := api.Resolve(context.Background(), id)
		if err != nil || app.AppID != "ap-test" || image.ImageID != id {
			t.Fatalf("Resolve(%s) = %v, %v, %v", id, app, image, err)
		}
	}
	if calls.apps.Load() != 1 || calls.images.Load() != 2 {
		t.Fatalf("app/image calls = %d/%d, want 1/2", calls.apps.Load(), calls.images.Load())
	}
}

func TestResolveRetriesFailuresAndCoalescesConcurrentLookups(t *testing.T) {
	api, calls := testSDKAPI(t)
	calls.fail.Store(true)
	if _, _, err := api.Resolve(context.Background(), "im-one"); err == nil {
		t.Fatal("lookup failure ignored")
	}
	calls.fail.Store(false)
	group, ctx := errgroup.WithContext(context.Background())
	for range 20 {
		group.Go(func() error { _, _, err := api.Resolve(ctx, "im-one"); return err })
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	apps, images := calls.apps.Load(), calls.images.Load()
	if apps > 2 || images > 2 {
		t.Fatalf("app/image calls = %d/%d, want at most 2/2 including failure", apps, images)
	}
}

type lookupServer struct {
	pb.UnimplementedModalClientServer
	apps, images atomic.Int64
	fail         atomic.Bool
}

func (s *lookupServer) AppGetOrCreate(ctx context.Context, _ *pb.AppGetOrCreateRequest) (*pb.AppGetOrCreateResponse, error) {
	s.apps.Add(1)
	if err := s.lookup(); err != nil {
		return nil, err
	}
	return pb.AppGetOrCreateResponse_builder{AppId: "ap-test"}.Build(), nil
}
func (s *lookupServer) ImageFromId(ctx context.Context, request *pb.ImageFromIdRequest) (*pb.ImageFromIdResponse, error) {
	s.images.Add(1)
	if err := s.lookup(); err != nil {
		return nil, err
	}
	return pb.ImageFromIdResponse_builder{ImageId: request.GetImageId()}.Build(), nil
}
func (s *lookupServer) lookup() error {
	if s.fail.Load() {
		return status.Error(codes.InvalidArgument, "lookup failed")
	}
	return nil
}

func testSDKAPI(t *testing.T) (*sdkAPI, *lookupServer) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	calls := &lookupServer{}
	pb.RegisterModalClientServer(server, calls)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///modal-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	client, err := modal.NewClientWithOptions(&modal.ClientParams{TokenID: "test", TokenSecret: "test", ControlPlaneClient: pb.NewModalClientClient(conn), ControlPlaneConn: conn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return newSDKAPI(client), calls
}
