package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProviderServerRetainsAndConsumesTheSpareAcrossCommands(t *testing.T) {
	sb := &fakeSandbox{}
	api := &fakeAPI{created: sb, findErr: errors.New("unexpected lookup")}
	client, _ := startProviderServer(t, func() (modalAPI, func(), error) { return api, func() {}, nil })
	call := func(operation string, request any, result any) error {
		t.Helper()
		document, _ := json.Marshal(map[string]any{"operation": operation, "request": request})
		reply, err := client.Post("http://provider/", "application/json", bytes.NewReader(document))
		if err != nil {
			t.Fatal(err)
		}
		defer reply.Body.Close()
		var value struct {
			Status string
			Error  string
			Result json.RawMessage
		}
		if err := json.NewDecoder(reply.Body).Decode(&value); err != nil {
			t.Fatal(err)
		}
		if value.Status != "success" {
			return errors.New(value.Error)
		}
		if err := json.Unmarshal(value.Result, result); err != nil {
			t.Fatal(err)
		}
		return nil
	}
	var spare spareHandle
	if err := call("create_spare", spareRequest{Kind: "actor", Name: "do-spare-test", ImageRef: "im-runtime", CanonicalRegion: "north-america-east", Resources: resourceLimits{CPUMillis: 1000, MemoryMiB: 1024}}, &spare); err != nil {
		t.Fatal(err)
	}
	createdCalls := len(sb.calls)
	if err := call("retire_spare", spare, &struct{}{}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sb.calls[createdCalls:], []string{"terminate", "detach"}) {
		t.Fatal("retained spare was not retired", sb.calls)
	}
	if err := call("retire_spare", spare, &struct{}{}); err == nil || err.Error() != api.findErr.Error() {
		t.Fatalf("retirement did not consume the cached handle: %v", err)
	}
}

type cancellableAPI struct {
	modalAPI
	started chan struct{}
	stopped chan struct{}
}

func (a *cancellableAPI) ByID(ctx context.Context, id string) (sandbox, error) {
	if id == "blocked" {
		close(a.started)
		<-ctx.Done()
		close(a.stopped)
		return nil, ctx.Err()
	}
	return &fakeSandbox{}, nil
}

func TestProviderServerSharesClientAndCancelsActiveRequests(t *testing.T) {
	for _, stop := range []string{"disconnect", "shutdown"} {
		t.Run(stop, func(t *testing.T) {
			api := &cancellableAPI{modalAPI: &fakeAPI{}, started: make(chan struct{}), stopped: make(chan struct{})}
			var opened, closed atomic.Int32
			factory := func() (modalAPI, func(), error) { opened.Add(1); return api, func() { closed.Add(1) }, nil }
			client, shutdown := startProviderServer(t, factory)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://provider/", strings.NewReader(`{"operation":"retire_spare","request":{"resourceId":"blocked"}}`))
			done := make(chan error, 1)
			go func() {
				response, err := client.Do(request)
				if response != nil {
					response.Body.Close()
				}
				done <- err
			}()
			waitSignal(t, api.started)
			assertProviderReply(t, client)
			if opened.Load() != 1 || closed.Load() != 0 {
				t.Fatal("client was not shared")
			}
			if stop == "disconnect" {
				cancel()
			} else {
				shutdown()
			}
			waitSignal(t, api.stopped)
			if err := <-done; stop == "disconnect" && err == nil {
				t.Fatal("cancelled request succeeded")
			}
			if stop == "disconnect" {
				assertProviderReply(t, client)
				if closed.Load() != 0 {
					t.Fatal("request cancellation closed the shared client")
				}
			}
			shutdown()
			if opened.Load() != 1 || closed.Load() != 1 {
				t.Fatal("provider did not release its single shared client")
			}
		})
	}
}

func startProviderServer(t *testing.T, factory apiFactory) (*http.Client, func()) {
	t.Helper()
	directory, err := os.MkdirTemp("", "da-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	ctx, cancel := context.WithCancel(context.Background())
	input, output := io.Pipe()
	done := make(chan error, 1)
	socket := filepath.Join(directory, "socket")
	go func() { defer output.Close(); done <- serveProvider(ctx, socket, output, factory, time.Now) }()
	line, err := bufio.NewReader(input).ReadString('\n')
	input.Close()
	if err != nil || line != "{\"protocol\":1}\n" {
		cancel()
		t.Fatalf("readiness: %q %v", line, err)
	}
	info, err := os.Stat(socket)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("provider socket is not private")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	stopped := false
	shutdown := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("provider did not stop")
		}
		transport.CloseIdleConnections()
	}
	t.Cleanup(shutdown)
	return client, shutdown
}

func assertProviderReply(t *testing.T, client *http.Client) {
	t.Helper()
	reply, err := client.Post("http://provider/", "application/json", strings.NewReader(`{"operation":"retire_spare","request":{"resourceId":"healthy"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer reply.Body.Close()
	var result response
	if err := json.NewDecoder(reply.Body).Decode(&result); err != nil || result.Status != "success" {
		t.Fatalf("healthy command failed: %+v %v", result, err)
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("operation timed out")
	}
}

type buildDeadlineSandbox struct{ fakeSandbox }

func (s *buildDeadlineSandbox) BuildCode(ctx context.Context, directory, entrypoint string) (json.RawMessage, error) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) < 11*time.Minute {
		return nil, errors.New("build request does not allow dependency installation and snapshotting")
	}
	return s.fakeSandbox.BuildCode(ctx, directory, entrypoint)
}

func TestBuildBudgetCoversDependencyInstallationThroughBothProviderTransports(t *testing.T) {
	for _, mode := range []string{"command", "server"} {
		t.Run(mode, func(t *testing.T) {
			api := &fakeAPI{created: &buildDeadlineSandbox{}}
			factory := func() (modalAPI, func(), error) { return api, func() {}, nil }
			input := `{"operation":"build_code","request":{"imageRef":"im-customer","workingDirectory":"/project","actorEntrypoint":"src/actors.py","canonicalRegion":"north-america-east"}}`
			var output bytes.Buffer
			if mode == "command" {
				if err := runCommand(context.Background(), strings.NewReader(input), &output, factory, time.Now); err != nil {
					t.Fatal(err)
				}
			} else {
				client, _ := startProviderServer(t, factory)
				reply, err := client.Post("http://provider/", "application/json", strings.NewReader(input))
				if err != nil {
					t.Fatal(err)
				}
				defer reply.Body.Close()
				if _, err := io.Copy(&output, reply.Body); err != nil {
					t.Fatal(err)
				}
			}
			var reply response
			if err := json.Unmarshal(output.Bytes(), &reply); err != nil || reply.Status != "success" {
				t.Fatalf("build failed: %s (%v)", output.String(), err)
			}
			if api.params.Timeout < 11*time.Minute {
				t.Fatal("builder sandbox expires before build completion")
			}
			seconds, err := strconv.Atoi(api.params.Command[1])
			if err != nil || seconds < 660 {
				t.Fatal("builder process exits before build completion")
			}
		})
	}
}
