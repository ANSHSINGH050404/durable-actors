package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	modal "github.com/modal-labs/modal-client/go"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

type sdkAPI struct {
	client     *modal.Client
	appMu      sync.Mutex
	app        *modal.App
	imageLoads singleflight.Group
	images     *lru.Cache[string, *modal.Image]
}

func newModalAPI() (modalAPI, func(), error) {
	mutable := false
	if value := os.Getenv("DURABLE_ACTORS_MODAL_MUTABLE_NETWORK"); value != "" {
		var err error
		mutable, err = strconv.ParseBool(value)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid mutable network configuration: %w", err)
		}
	}
	// The Rust parent supplies credentials in a sanitized environment without HOME.
	if os.Getenv("HOME") == "" {
		if err := os.Setenv("MODAL_CONFIG_PATH", os.DevNull); err != nil {
			return nil, nil, err
		}
	}
	client, err := modal.NewClient()
	if err != nil {
		return nil, nil, err
	}
	return &networkPolicyAPI{modalAPI: newSDKAPI(client), mutable: mutable}, client.Close, nil
}

func newSDKAPI(client *modal.Client) *sdkAPI {
	images, err := lru.New[string, *modal.Image](128)
	if err != nil {
		panic(err)
	}
	return &sdkAPI{client: client, images: images}
}

func (a *sdkAPI) Resolve(ctx context.Context, imageID string) (*modal.App, *modal.Image, error) {
	group, ctx := errgroup.WithContext(ctx)
	var app *modal.App
	var image *modal.Image
	group.Go(func() error {
		var err error
		app, err = a.resolveApp(ctx)
		return err
	})
	group.Go(func() error { var err error; image, err = a.resolveImage(ctx, imageID); return err })
	err := group.Wait()
	return app, image, err
}

func (a *sdkAPI) resolveApp(ctx context.Context) (*modal.App, error) {
	a.appMu.Lock()
	defer a.appMu.Unlock()
	if a.app != nil {
		return a.app, nil
	}
	app, err := a.client.Apps.FromName(ctx, appName, &modal.AppFromNameParams{CreateIfMissing: true})
	if err == nil {
		a.app = app
	}
	return app, err
}

func (a *sdkAPI) resolveImage(ctx context.Context, id string) (*modal.Image, error) {
	if image, ok := a.images.Get(id); ok {
		return image, nil
	}
	result := a.imageLoads.DoChan(id, func() (any, error) {
		if image, ok := a.images.Get(id); ok {
			return image, nil
		}
		image, err := a.client.Images.FromID(ctx, id, nil)
		if err == nil {
			a.images.Add(id, image)
		}
		return image, err
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-result:
		if result.Err != nil {
			return nil, result.Err
		}
		return result.Val.(*modal.Image), nil
	}
}

func (a *sdkAPI) Create(ctx context.Context, app *modal.App, image *modal.Image, params *modal.SandboxCreateParams) (sandbox, error) {
	sb, err := a.client.Sandboxes.ExperimentalCreate(ctx, app, image, params)
	if err != nil {
		return nil, err
	}
	return &sdkSandbox{sb}, nil
}

func (a *sdkAPI) Find(ctx context.Context, name string) (sandbox, error) {
	sb, err := a.client.Sandboxes.ExperimentalFromName(ctx, appName, name, nil)
	if err != nil {
		return nil, err
	}
	return &sdkSandbox{sb}, nil
}

func (a *sdkAPI) ByID(ctx context.Context, id string) (sandbox, error) {
	sb, err := a.client.Sandboxes.FromID(ctx, id, nil)
	if err != nil {
		return nil, err
	}
	return &sdkSandbox{sb}, nil
}

func (a *sdkAPI) Secret(ctx context.Context, name string) (*modal.Secret, error) {
	return a.client.Secrets.FromName(ctx, name, nil)
}

type sdkSandbox struct{ sb *modal.Sandbox }

func (s *sdkSandbox) ID() string { return s.sb.SandboxID }
func (s *sdkSandbox) Detach()    { _ = s.sb.Detach() }
func (s *sdkSandbox) Terminate(ctx context.Context) error {
	_, err := s.sb.Terminate(ctx, nil)
	return err
}
func (s *sdkSandbox) Ready(ctx context.Context) error {
	return s.sb.WaitUntilReady(ctx, time.Minute, nil)
}
func (s *sdkSandbox) Routes(ctx context.Context) (string, string, error) {
	tunnels, err := s.sb.Tunnels(ctx, 50*time.Second, nil)
	if err != nil {
		return "", "", err
	}
	route, control := tunnels[7101], tunnels[7102]
	if route == nil || control == nil {
		return "", "", fmt.Errorf("Modal did not create the durable-actors HTTP/2 tunnels")
	}
	return route.URL(), control.URL(), nil
}

func (s *sdkSandbox) Metadata(ctx context.Context) ([]byte, error) {
	command := "for i in $(seq 1 1200); do test -f " + readyFile + " && test -s " + metadataFile + " && exec cat " + metadataFile + "; sleep 0.05; done; exit 1"
	process, err := s.sb.Exec(ctx, []string{"sh", "-c", command}, &modal.SandboxExecParams{Stdout: modal.Pipe, Stderr: modal.Ignore, Timeout: time.Minute})
	if err != nil {
		return nil, err
	}
	defer process.Stdout.Close()
	document, err := io.ReadAll(io.LimitReader(process.Stdout, maximumCommandBytes+1))
	if err != nil {
		return nil, err
	}
	if len(document) > maximumCommandBytes {
		return nil, fmt.Errorf("host metadata is too large")
	}
	exitCode, err := process.Wait(ctx, nil)
	if err != nil {
		return nil, err
	}
	if exitCode != 0 {
		return nil, fmt.Errorf("existing Modal host has no ready metadata")
	}
	return document, nil
}

func (s *sdkSandbox) Connect(ctx context.Context) (socketCredentials, error) {
	credentials, err := s.sb.CreateConnectToken(ctx, &modal.SandboxCreateConnectTokenParams{Port: 7101})
	if err != nil {
		return socketCredentials{}, err
	}
	return socketCredentials{URL: credentials.URL, Token: credentials.Token}, nil
}

func (s *sdkSandbox) Mount(ctx context.Context, snapshotID string) error {
	return s.sb.MountImage(ctx, "/customer", &modal.Image{ImageID: snapshotID}, nil)
}

func (s *sdkSandbox) Snapshot(ctx context.Context) (string, error) {
	image, err := s.sb.SnapshotDirectory(ctx, compiledCodeDirectory, &modal.SandboxSnapshotDirectoryParams{TTL: modal.NoExpiryTTL})
	if err != nil {
		return "", err
	}
	return image.ImageID, nil
}

func (s *sdkSandbox) BuildCode(ctx context.Context, directory, entrypoint string) (json.RawMessage, error) {
	process, err := s.sb.Exec(ctx, []string{"bun", "/opt/durable-actors/sdk/dist/compiler/deployment-build.js", directory, entrypoint, compiledCodeDirectory}, &modal.SandboxExecParams{Stdout: modal.Pipe, Stderr: modal.Pipe, Timeout: actorBuildTimeout})
	if err != nil {
		return nil, fmt.Errorf("start actor compiler (build image requires matching runtime and actor SDKs): %w", err)
	}
	defer process.Stdout.Close()
	defer process.Stderr.Close()
	var output, diagnostics []byte
	var exit int
	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		output, err = io.ReadAll(io.LimitReader(process.Stdout, maximumContractBytes+1))
		return err
	})
	group.Go(func() error {
		var err error
		diagnostics, err = io.ReadAll(io.LimitReader(process.Stderr, maximumCommandBytes+1))
		return err
	})
	group.Go(func() error { var err error; exit, err = process.Wait(ctx, nil); return err })
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if exit != 0 {
		return nil, fmt.Errorf("actor compilation failed: %s", diagnostics)
	}
	if len(output) > maximumContractBytes || !json.Valid(output) {
		return nil, fmt.Errorf("actor compiler returned an invalid or oversized contract")
	}
	return json.RawMessage(output), nil
}
