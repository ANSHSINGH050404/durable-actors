package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const maximumCommandBytes = 1024 * 1024
const maximumContractBytes = 4 * 1024 * 1024
const maximumResponseBytes = maximumContractBytes + 1024*1024

type apiFactory func() (modalAPI, func(), error)
type command struct {
	Operation string          `json:"operation"`
	Request   json.RawMessage `json:"request"`
}
type response struct {
	Status string `json:"status"`
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

type commandRunner struct {
	api      modalAPI
	handles  *spareHandles
	assigner spareAssigner
	now      func() time.Time
}

func newCommandRunner(api modalAPI, now func() time.Time) *commandRunner {
	return &commandRunner{api: api, handles: newSpareHandles(128, time.Hour, now), assigner: httpSpareAssigner{client: newAssignmentClient()}, now: now}
}

func runCommand(ctx context.Context, input io.Reader, output io.Writer, factory apiFactory, now func() time.Time) error {
	result, err := executeCommand(ctx, input, factory, now)
	return writeReply(output, result, err)
}

func executeCommand(ctx context.Context, input io.Reader, factory apiFactory, now func() time.Time) (any, error) {
	started := now()
	cmd, err := readCommand(input)
	if err != nil {
		return nil, err
	}
	parsed := elapsed(started, now())
	api, closeClient, err := factory()
	if err != nil {
		return nil, err
	}
	defer closeClient()
	runner := newCommandRunner(api, now)
	defer runner.handles.close()
	return runner.execute(ctx, cmd, started, parsed)
}

func (r *commandRunner) execute(ctx context.Context, cmd command, started time.Time, parsed int64) (any, error) {
	p := &provider{assigner: r.assigner, api: r.api, handles: r.handles, now: r.now, started: started, inputParsed: parsed, sdkLoaded: elapsed(started, r.now())}
	switch cmd.Operation {
	case "create_spare":
		var request spareRequest
		if err := json.Unmarshal(cmd.Request, &request); err != nil {
			return nil, err
		}
		return p.createSpare(ctx, request)
	case "retire_spare":
		var request spareHandle
		if err := json.Unmarshal(cmd.Request, &request); err != nil {
			return nil, err
		}
		return struct{}{}, p.retireSpare(ctx, request)
	case "build_code":
		var request buildCodeRequest
		if err := json.Unmarshal(cmd.Request, &request); err != nil {
			return nil, err
		}
		return p.buildCode(ctx, request)
	case "socket_credentials":
		var request socketRequest
		if err := json.Unmarshal(cmd.Request, &request); err != nil {
			return nil, err
		}
		return p.socketCredentials(ctx, request)
	case "ensure_host":
		var request ensureRequest
		if err := json.Unmarshal(cmd.Request, &request); err != nil {
			return nil, err
		}
		return p.ensureHost(ctx, request)
	default:
		return nil, fmt.Errorf("unsupported sandbox operation")
	}
}

func writeReply(output io.Writer, result any, failure error) error {
	reply := response{Status: "success", Result: result}
	if failure != nil {
		reply = response{Status: "failure", Error: failure.Error()}
	}
	document, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	if len(document) >= maximumResponseBytes {
		return fmt.Errorf("provider response is too large")
	}
	_, err = output.Write(append(document, '\n'))
	return err
}

func readCommand(input io.Reader) (command, error) {
	document, err := io.ReadAll(io.LimitReader(input, maximumCommandBytes+1))
	if err != nil {
		return command{}, err
	}
	if len(document) > maximumCommandBytes {
		return command{}, fmt.Errorf("provider command exceeds %d bytes", maximumCommandBytes)
	}
	var cmd command
	if err := json.Unmarshal(document, &cmd); err != nil {
		return cmd, err
	}
	switch cmd.Operation {
	case "ensure_host", "socket_credentials", "create_spare", "retire_spare", "build_code":
		return cmd, nil
	default:
		return cmd, fmt.Errorf("unsupported sandbox operation")
	}
}

func elapsed(start, finish time.Time) int64 { return max(0, finish.Sub(start).Milliseconds()) }
