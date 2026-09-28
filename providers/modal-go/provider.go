package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	modal "github.com/modal-labs/modal-client/go"
)

const (
	appName      = "durable-actors-hosts"
	metadataFile = "/tmp/durable-actors-host.json"
	readyFile    = "/tmp/durable-actors-ready"
)

type modalAPI interface {
	Resolve(context.Context, string) (*modal.App, *modal.Image, error)
	Secret(context.Context, string) (*modal.Secret, error)
	Create(context.Context, *modal.App, *modal.Image, *modal.SandboxCreateParams) (sandbox, error)
	Find(context.Context, string) (sandbox, error)
	ByID(context.Context, string) (sandbox, error)
}

type sandbox interface {
	ID() string
	Routes(context.Context) (string, string, error)
	Connect(context.Context) (socketCredentials, error)
	Mount(context.Context, string) error
	Snapshot(context.Context) (string, error)
	BuildCode(context.Context, string, string) (json.RawMessage, error)
	Ready(context.Context) error
	Metadata(context.Context) ([]byte, error)
	Terminate(context.Context) error
	Detach()
}

type provider struct {
	assigner               spareAssigner
	api                    modalAPI
	handles                *spareHandles
	now                    func() time.Time
	started                time.Time
	inputParsed, sdkLoaded int64
}

func (p *provider) sandboxByID(ctx context.Context, id string) (sandbox, error) {
	if sb := p.handles.take(id); sb != nil {
		return sb, nil
	}
	return p.api.ByID(ctx, id)
}

func (p *provider) socketCredentials(ctx context.Context, request socketRequest) (socketCredentials, error) {
	if request.ResourceID == "" || request.HostID == "" || request.SessionID == "" {
		return socketCredentials{}, fmt.Errorf("invalid socket host identity")
	}
	if _, err := modalRegion(request.CanonicalRegion); err != nil {
		return socketCredentials{}, err
	}
	sb, err := p.api.ByID(ctx, request.ResourceID)
	if err != nil {
		return socketCredentials{}, err
	}
	defer sb.Detach()
	document, err := sb.Metadata(ctx)
	if err != nil {
		return socketCredentials{}, err
	}
	var metadata struct {
		HostID          string `json:"hostId"`
		SessionID       string `json:"sessionId"`
		CanonicalRegion string `json:"canonicalRegion"`
	}
	if err := json.Unmarshal(document, &metadata); err != nil {
		return socketCredentials{}, err
	}
	if metadata.HostID != request.HostID || metadata.SessionID != request.SessionID || metadata.CanonicalRegion != request.CanonicalRegion {
		return socketCredentials{}, fmt.Errorf("socket host session was replaced; resolve a new target")
	}
	return sb.Connect(ctx)
}

func terminateForCleanup(sb sandbox) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = sb.Terminate(ctx)
}

func (p *provider) elapsed() int64 { return elapsed(p.started, p.now()) }
