package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

func serveProvider(ctx context.Context, socket string, ready io.Writer, factory apiFactory, now func() time.Time) error {
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := os.Chmod(socket, 0600); err != nil {
		return err
	}
	api, closeClient, err := factory()
	if err != nil {
		return err
	}
	defer closeClient()
	runner := newCommandRunner(api, now)
	defer runner.handles.close()
	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()
	go runner.handles.maintain(lifetime)
	server := &http.Server{
		Handler: providerHandler(runner), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: providerRequestTimeout, WriteTimeout: actorBuildRequestTimeout + 5*time.Second, MaxHeaderBytes: 8192,
		BaseContext: func(net.Listener) context.Context { return lifetime },
	}
	defer server.Close()
	if _, err := io.WriteString(ready, "{\"protocol\":1}\n"); err != nil {
		return err
	}
	return serveUntilCancelled(ctx, server, listener)
}

func providerHandler(runner *commandRunner) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/" {
			http.NotFound(w, request)
			return
		}
		started := runner.now()
		cmd, err := readCommand(request.Body)
		var result any
		if err == nil {
			result, err = runner.execute(request.Context(), cmd, started, elapsed(started, runner.now()))
		}
		w.Header().Set("Content-Type", "application/json")
		if err := writeReply(w, result, err); err != nil {
			http.Error(w, "provider response failed", http.StatusInternalServerError)
		}
	})
}

func serveUntilCancelled(ctx context.Context, server *http.Server, listener net.Listener) error {
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}
