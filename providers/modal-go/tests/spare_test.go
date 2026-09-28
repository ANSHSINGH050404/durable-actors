package main

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestSparePlacementUsesGCP(t *testing.T) {
	for _, test := range []struct{ kind, region, placement string }{{"actor", "north-america-east", "us-east"}, {"replica", "europe-west", "eu-west"}, {"actor", "canada", "ca"}} {
		t.Run(test.kind, func(t *testing.T) {
			params, err := spareParams(spareRequest{Kind: test.kind, Name: "do-spare-test", ImageRef: "im-runtime", CanonicalRegion: test.region, Resources: resourceLimits{CPUMillis: 1000, MemoryMiB: 1024}})
			if err != nil {
				t.Fatal(err)
			}
			if params.Cloud != "gcp" || !slices.Equal(params.Regions, []string{test.placement}) {
				t.Fatalf("unexpected placement: %v %v", params.Cloud, params.Regions)
			}
		})
	}
}

func TestSpareRetainsItsConnectionUntilClaimed(t *testing.T) {
	sb := &fakeSandbox{}
	api := &fakeAPI{created: sb, found: sb}
	p := newTestProvider(api)
	spare, err := p.createSpare(context.Background(), spareRequest{Kind: "actor", Name: "do-spare-test", ImageRef: "im-runtime", CanonicalRegion: "north-america-east", Resources: resourceLimits{CPUMillis: 1000, MemoryMiB: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(sb.calls, "detach") {
		t.Fatal("ready spare connection was discarded")
	}
	request := testRequest()
	request.Spare = &spare
	if _, err := p.ensureHost(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if api.finds != 0 {
		t.Fatal("claimed spare was looked up again")
	}
	if !slices.Contains(sb.calls, "detach") {
		t.Fatal("claimed handle was not released")
	}
}

func TestGenericSpareHasNoCustomerCredentialsAndAppliesLimits(t *testing.T) {
	api := &fakeAPI{created: &fakeSandbox{}}
	p := newTestProvider(api)
	handle, err := p.createSpare(context.Background(), spareRequest{
		Kind: "actor", Name: "do-spare-test", ImageRef: "im-runtime", CanonicalRegion: "north-america-east",
		Resources: resourceLimits{CPUMillis: 2000, MemoryMiB: 2048},
	})
	if err != nil {
		t.Fatal(err)
	}
	if handle.ResourceID == "" || handle.Route == "" || handle.ControlRoute == "" || len(handle.ControlToken) != 64 {
		t.Fatal(handle)
	}
	if api.params.CPU != 2 || api.params.CPULimit != 2 || api.params.MemoryMiB != 2048 || api.params.MemoryLimitMiB != 2048 {
		t.Fatal("missing Modal hard limits")
	}
	if api.params.Env["DURABLE_ACTORS_HOST_TOKEN"] != "" || api.params.Env["DURABLE_ACTORS_ENTRYPOINT"] != "" || len(api.params.Secrets) != 0 {
		t.Fatal("spare is customer-bound before claim")
	}
	if api.params.Env["DURABLE_ACTORS_PROCESS_ROLE"] != "spare" {
		t.Fatal("runtime is not initialized")
	}
}

func TestGenericAssignmentMountsCodeAndAssignsExactlyOneActor(t *testing.T) {
	request := testRequest()
	request.HostIdleTimeoutMS = 75000
	request.ActorIsNew = true
	request.OwnerHint = `{"generation":17,"record":{"epoch":3}}`
	request.Actor = json.RawMessage(`{"project_id":"default","actor_name":"Counter","actor_id":"one"}`)
	request.CodeSnapshot = "im-code"
	request.WorkingDirectory = "/customer"
	request.ActorEntrypoint = "actors.mjs"
	request.Spare = &spareHandle{Name: "do-spare-test", ResourceID: "sb-test", Route: "https://host.test", CanonicalRegion: request.CanonicalRegion}
	sb := &fakeSandbox{}
	api := &fakeAPI{found: sb}
	handle, err := newTestProvider(api).ensureHost(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if handle.OwnerEpoch != 42 || handle.Lease == nil || sb.assignment["DURABLE_ACTORS_ACTOR_IS_NEW"] != "true" {
		t.Fatal("ownership epoch missing")
	}
	if sb.assignment["DURABLE_ACTORS_OWNER_HINT"] != request.OwnerHint {
		t.Fatal("owner hint missing from assignment")
	}
	if api.creates != 0 {
		t.Fatal("claimed spare was replaced by a new sandbox")
	}
	if sb.mounted != "im-code" || sb.assignment["DURABLE_ACTORS_ACTOR"] != string(request.Actor) {
		t.Fatal("missing code or actor binding")
	}
	if sb.assignment["DURABLE_ACTORS_ENTRYPOINT"] != "/customer/actors.mjs" {
		t.Fatal(sb.assignment)
	}
	if sb.assignment["DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS"] != "75000" {
		t.Fatal("host idle timeout was not passed to the assigned sandbox")
	}
}

func TestAssignmentRejectsReadinessFromAnotherSessionOrMissingEpoch(t *testing.T) {
	for _, invalid := range []string{"session", "epoch"} {
		request := testRequest()
		request.Spare = &spareHandle{ResourceID: "sb-test", Route: "https://host.test", CanonicalRegion: request.CanonicalRegion}
		handle := hostHandle{HostID: request.HostID, SessionID: request.SessionID, Route: request.Spare.Route, CanonicalRegion: request.CanonicalRegion, OwnerEpoch: 42}
		if invalid == "session" {
			handle.SessionID = "another-session"
		} else {
			handle.OwnerEpoch = 0
		}
		document, _ := json.Marshal(handle)
		sb := &fakeSandbox{metadata: string(document)}
		if _, err := newTestProvider(&fakeAPI{found: sb}).ensureHost(context.Background(), request); err == nil {
			t.Fatal(invalid)
		}
		if len(sb.calls) < 1 || sb.calls[0] != "terminate" {
			t.Fatal("failed activation leaked sandbox")
		}
	}
}

func TestAssignmentRejectsInvalidActivationLease(t *testing.T) {
	for _, invalid := range []string{"missing", "host", "session", "route", "expired"} {
		request := testRequest()
		request.Spare = &spareHandle{ResourceID: "sb-test", Route: "https://host.test", CanonicalRegion: request.CanonicalRegion}
		lease := &activationLease{ID: request.HostID, SessionID: request.SessionID, Route: request.Spare.Route, ExpiresAtMS: uint64(time.Now().Add(time.Minute).UnixMilli())}
		switch invalid {
		case "missing":
			lease = nil
		case "host":
			lease.ID = "other"
		case "session":
			lease.SessionID = "other"
		case "route":
			lease.Route = "https://other.test"
		case "expired":
			lease.ExpiresAtMS = 1
		}
		handle := hostHandle{Lease: lease, HostID: request.HostID, SessionID: request.SessionID, Route: request.Spare.Route, CanonicalRegion: request.CanonicalRegion, OwnerEpoch: 42}
		document, _ := json.Marshal(handle)
		sb := &fakeSandbox{metadata: string(document)}
		if _, err := newTestProvider(&fakeAPI{found: sb}).ensureHost(context.Background(), request); err == nil {
			t.Fatalf("accepted %s lease", invalid)
		}
	}
}

func TestAssignmentRequiresProjectAndActorName(t *testing.T) {
	for _, identity := range []string{
		`{"actor_name":"Counter","actor_id":"one"}`,
		`{"project_id":"default","actor_type":"Counter","actor_id":"one"}`,
	} {
		request := testRequest()
		request.Actor = json.RawMessage(identity)
		if err := validateAssignment(request); err == nil {
			t.Fatalf("accepted incomplete actor identity: %s", identity)
		}
	}
}

func TestOnlyActorSparesReceiveTheControlPlaneForPreconnection(t *testing.T) {
	for _, kind := range []string{"actor", "replica"} {
		params, err := spareParams(spareRequest{Kind: kind, Name: "spare", ImageRef: "im-runtime", CanonicalRegion: "north-america-east", ControlPlaneURL: "https://control.test", Resources: resourceLimits{CPUMillis: 1000, MemoryMiB: 1024}})
		if err != nil {
			t.Fatal(err)
		}
		expected := ""
		if kind == "actor" {
			expected = "https://control.test"
		}
		if actual := params.Env["DURABLE_ACTORS_CONTROL_PLANE_URL"]; actual != expected {
			t.Fatalf("%s control plane = %q, want %q", kind, actual, expected)
		}
	}
}

func TestPoolMissOverlapsStartupAndCleansUpFailure(t *testing.T) {
	for _, want := range []error{nil, errors.New("startup failed")} {
		started, release := make(chan struct{}, 2), make(chan struct{})
		sb := &fakeSandbox{mountStarted: make(chan struct{}), assignmentStarted: make(chan struct{})}
		sb.startup = func(ctx context.Context) error {
			started <- struct{}{}
			select {
			case <-release:
				return want
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		p := newTestProvider(&fakeAPI{created: sb})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		done := make(chan error, 1)
		go func() { _, err := p.ensureHost(ctx, testRequest()); done <- err }()
		for _, phase := range []<-chan struct{}{sb.mountStarted, started, started} {
			select {
			case <-phase:
			case <-ctx.Done():
				cancel()
				<-done
				t.Fatal("mount, readiness, and routes did not overlap")
			}
		}
		select {
		case <-sb.assignmentStarted:
			t.Error("assigned before readiness and routes")
		default:
		}
		close(release)
		err := <-done
		cancel()
		if !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
		if sb.mounted != testRequest().CodeSnapshot {
			t.Fatalf("mounted %q", sb.mounted)
		}
		if want != nil && (len(sb.calls) != 4 || !slices.Equal(sb.calls[2:], []string{"terminate", "detach"})) {
			t.Fatalf("cleanup = %v", sb.calls)
		}
	}
}
