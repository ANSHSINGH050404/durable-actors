package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	modal "github.com/modal-labs/modal-client/go"
	"golang.org/x/sync/errgroup"
)

const spareReadyFile = "/tmp/durable-actors-spare-ready"
const compiledCodeDirectory = "/tmp/durable-actors-code"

type resourceLimits struct {
	CPUMillis int `json:"cpuMillis"`
	MemoryMiB int `json:"memoryMib"`
}
type spareRequest struct {
	ControlPlaneURL string         `json:"controlPlaneUrl,omitempty"`
	Kind            string         `json:"kind"`
	Name            string         `json:"name"`
	ImageRef        string         `json:"imageRef"`
	CanonicalRegion string         `json:"canonicalRegion"`
	Resources       resourceLimits `json:"resources"`
}
type spareHandle struct {
	ControlRoute    string `json:"controlRoute,omitempty"`
	ControlToken    string `json:"controlToken,omitempty"`
	Name            string `json:"name"`
	ResourceID      string `json:"resourceId"`
	Route           string `json:"route"`
	CanonicalRegion string `json:"canonicalRegion"`
}

func (p *provider) ensureHost(ctx context.Context, request ensureRequest) (hostHandle, error) {
	if err := validateAssignment(request); err != nil {
		return hostHandle{}, err
	}
	phases := &provisioning{Provider: "modal", StartedAtMS: p.elapsed(), InputParsedAtMS: p.inputParsed, SDKLoadedAtMS: p.sdkLoaded}
	var sb sandbox
	var err error
	var spare spareHandle
	if request.Spare != nil {
		if len(request.SecretRefs) != 0 || request.Spare.CanonicalRegion != request.CanonicalRegion {
			return hostHandle{}, fmt.Errorf("spare scope mismatch")
		}
		sb, err = p.sandboxByID(ctx, request.Spare.ResourceID)
		spare = *request.Spare
		phases.Reused = true
	} else {
		sb, spare, err = p.createSpareSandbox(ctx, spareRequest{Kind: "actor", Name: "do-actor-" + request.SessionID, ImageRef: request.ImageRef, CanonicalRegion: request.CanonicalRegion, Resources: request.Resources}, request.SecretRefs)
	}
	if err != nil {
		return hostHandle{}, err
	}
	defer sb.Detach()
	succeeded := false
	defer func() {
		if !succeeded {
			terminateForCleanup(sb)
		}
	}()
	phases.SandboxScheduledAtMS = p.elapsed()
	group, assignmentContext := errgroup.WithContext(ctx)
	group.Go(func() error { return sb.Mount(assignmentContext, request.CodeSnapshot) })
	var handle hostHandle
	group.Go(func() error {
		if request.Spare == nil {
			if err := readySpare(assignmentContext, sb, &spare); err != nil {
				return err
			}
		}
		environment := hostEnvironment(request)
		environment["DURABLE_ACTORS_HOST_ROUTE"] = spare.Route
		environment["DURABLE_ACTORS_ENTRYPOINT"] = path.Join("/customer", request.ActorEntrypoint)
		var err error
		handle, err = p.assigner.Assign(assignmentContext, spare, environment)
		return err
	})
	if err := group.Wait(); err != nil {
		return hostHandle{}, err
	}
	if handle.HostID != request.HostID || handle.SessionID != request.SessionID || handle.Route != spare.Route || handle.CanonicalRegion != request.CanonicalRegion || handle.OwnerEpoch == 0 {
		return hostHandle{}, fmt.Errorf("assigned sandbox identity or ownership mismatch")
	}
	lease := handle.Lease
	if lease == nil || lease.ID != request.HostID || lease.SessionID != request.SessionID || lease.Route != spare.Route || lease.ExpiresAtMS <= uint64(time.Now().UnixMilli()) {
		return hostHandle{}, fmt.Errorf("assigned sandbox activation lease invalid")
	}
	phases.ResourceID = sb.ID()
	phases.HostReadyObservedAtMS = p.elapsed()
	phases.CompletedAtMS = p.elapsed()
	handle.Provisioning = phases
	succeeded = true
	return handle, nil
}

func (p *provider) createSpare(ctx context.Context, request spareRequest) (spareHandle, error) {
	sb, handle, err := p.createSpareSandbox(ctx, request, nil)
	if err != nil {
		return spareHandle{}, err
	}
	if err := readySpare(ctx, sb, &handle); err != nil {
		terminateForCleanup(sb)
		sb.Detach()
		return spareHandle{}, err
	}
	if request.Kind == "actor" {
		p.handles.keep(sb)
		warm, cancel := context.WithTimeout(ctx, 3*time.Second)
		p.assigner.Warm(warm, handle)
		cancel()
	} else {
		sb.Detach()
	}
	return handle, nil
}

func (p *provider) createSpareSandbox(ctx context.Context, request spareRequest, secrets []string) (sandbox, spareHandle, error) {
	params, err := spareParams(request)
	if err != nil {
		return nil, spareHandle{}, err
	}
	app, image, err := p.api.Resolve(ctx, request.ImageRef)
	if err != nil {
		return nil, spareHandle{}, err
	}
	for _, name := range secrets {
		secret, err := p.api.Secret(ctx, name)
		if err != nil {
			return nil, spareHandle{}, err
		}
		params.Secrets = append(params.Secrets, secret)
	}
	sb, err := p.api.Create(ctx, app, image, params)
	if err != nil {
		return nil, spareHandle{}, err
	}
	return sb, spareHandle{Name: request.Name, ResourceID: sb.ID(), CanonicalRegion: request.CanonicalRegion, ControlToken: params.Env["DURABLE_ACTORS_SPARE_TOKEN"]}, nil
}

func readySpare(ctx context.Context, sb sandbox, handle *spareHandle) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return sb.Ready(ctx) })
	group.Go(func() error {
		route, control, err := sb.Routes(ctx)
		if err != nil {
			return err
		}
		if route == "" || control == "" {
			return fmt.Errorf("spare routes unavailable")
		}
		handle.Route, handle.ControlRoute = route, control
		return nil
	})
	return group.Wait()
}

func (p *provider) retireSpare(ctx context.Context, request spareHandle) error {
	var sb sandbox
	var err error
	if request.ResourceID != "" {
		sb, err = p.sandboxByID(ctx, request.ResourceID)
	} else {
		sb, err = p.api.Find(ctx, request.Name)
	}
	var missing modal.NotFoundError
	if errors.As(err, &missing) {
		return nil
	}
	if err != nil {
		return err
	}
	if cached := p.handles.take(sb.ID()); cached != nil {
		cached.Detach()
	}
	defer sb.Detach()
	return sb.Terminate(ctx)
}

func spareParams(request spareRequest) (*modal.SandboxCreateParams, error) {
	if request.Name == "" || request.ImageRef == "" {
		return nil, fmt.Errorf("spare identity and runtime image are required")
	}
	role := "spare"
	switch request.Kind {
	case "actor":
	case "replica":
		role = "replica"
	default:
		return nil, fmt.Errorf("invalid spare kind")
	}
	limits := request.Resources
	if limits.CPUMillis < 100 || limits.CPUMillis > 64000 || limits.MemoryMiB < 128 || limits.MemoryMiB > 262144 {
		return nil, fmt.Errorf("invalid sandbox resource limits")
	}
	region, err := modalRegion(request.CanonicalRegion)
	if err != nil {
		return nil, err
	}
	probe, err := modal.NewExecProbe([]string{"test", "-f", spareReadyFile}, &modal.ExecProbeParams{Interval: 50 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	environment := map[string]string{"DURABLE_ACTORS_PROCESS_ROLE": role, "DURABLE_ACTORS_SPARE_TOKEN": hex.EncodeToString(token)}
	if request.Kind == "actor" && request.ControlPlaneURL != "" {
		environment["DURABLE_ACTORS_CONTROL_PLANE_URL"] = request.ControlPlaneURL
	}
	return &modal.SandboxCreateParams{
		Name: request.Name, Timeout: 24 * time.Hour, Workdir: "/opt/durable-actors",
		Command: []string{"sh", "-c", "exec /usr/local/bin/durable-actors 2> /tmp/durable-actors-host.stderr"},
		Env:     environment,
		H2Ports: []int{7101, 7102}, ReadinessProbe: probe, Regions: []string{region}, Cloud: "gcp",
		CPU: float64(limits.CPUMillis) / 1000, CPULimit: float64(limits.CPUMillis) / 1000,
		MemoryMiB: limits.MemoryMiB, MemoryLimitMiB: limits.MemoryMiB,
	}, nil
}

func validateAssignment(request ensureRequest) error {
	if err := validateEnsure(request); err != nil {
		return err
	}
	var actor struct {
		ProjectID string `json:"project_id"`
		Name      string `json:"actor_name"`
		ID        string `json:"actor_id"`
	}
	if err := json.Unmarshal(request.Actor, &actor); err != nil || actor.ProjectID == "" || actor.Name == "" || actor.ID == "" {
		return fmt.Errorf("exactly one actor identity is required")
	}
	if !strings.HasPrefix(request.CodeSnapshot, "im-") {
		return fmt.Errorf("published code snapshot is required")
	}
	entrypoint := request.ActorEntrypoint
	if request.WorkingDirectory != "/customer" || entrypoint == "" || strings.HasPrefix(entrypoint, "/") || path.Clean(entrypoint) != entrypoint || strings.HasPrefix(entrypoint, "../") || !strings.HasSuffix(entrypoint, ".mjs") {
		return fmt.Errorf("customer entrypoint must be a compiled module under /customer")
	}
	return nil
}

type buildCodeRequest struct {
	ImageRef         string `json:"imageRef"`
	CanonicalRegion  string `json:"canonicalRegion"`
	WorkingDirectory string `json:"workingDirectory"`
	ActorEntrypoint  string `json:"actorEntrypoint"`
}

func (p *provider) buildCode(ctx context.Context, request buildCodeRequest) (map[string]any, error) {
	if !strings.HasPrefix(request.ImageRef, "im-") || request.WorkingDirectory == "" || !strings.HasPrefix(request.WorkingDirectory, "/") || request.ActorEntrypoint == "" {
		return nil, fmt.Errorf("actor build requires a customer image, absolute project directory, and entrypoint")
	}
	region, err := modalRegion(request.CanonicalRegion)
	if err != nil {
		return nil, err
	}
	app, image, err := p.api.Resolve(ctx, request.ImageRef)
	if err != nil {
		return nil, err
	}
	sb, err := p.api.Create(ctx, app, image, &modal.SandboxCreateParams{Command: []string{"sleep", "120"}, Timeout: 2 * time.Minute, Regions: []string{region}, Cloud: "gcp", CPU: 1, CPULimit: 1})
	if err != nil {
		return nil, err
	}
	defer sb.Detach()
	defer terminateForCleanup(sb)
	contract, err := sb.BuildCode(ctx, request.WorkingDirectory, request.ActorEntrypoint)
	if err != nil {
		return nil, err
	}
	snapshot, err := sb.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"codeSnapshot": snapshot, "contract": contract}, nil
}
