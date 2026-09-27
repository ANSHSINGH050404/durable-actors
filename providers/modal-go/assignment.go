package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type spareAssigner interface {
	Assign(context.Context, spareHandle, map[string]string) (hostHandle, error)
	Warm(context.Context, spareHandle)
}

type httpSpareAssigner struct{ client *http.Client }

var modalTunnelRoute = regexp.MustCompile(`^https://[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*\.modal\.host/?$`)

// Keep idle tunnel connections for the maximum configured spare lifetime.
func newAssignmentClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.IdleConnTimeout = time.Hour
	transport.MaxIdleConnsPerHost = 2
	return &http.Client{Transport: transport, Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// Warm opens the tunnel connection that a later assignment reuses. The spare
// rejects the unauthenticated GET, so no credentials are sent and nothing runs.
func (a httpSpareAssigner) Warm(ctx context.Context, spare spareHandle) {
	if !modalTunnelRoute.MatchString(spare.ControlRoute) {
		return
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(spare.ControlRoute, "/")+"/assign", nil)
	if err != nil {
		return
	}
	response, err := a.client.Do(request)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
}

func (a httpSpareAssigner) Assign(ctx context.Context, spare spareHandle, environment map[string]string) (hostHandle, error) {
	if spare.ControlRoute == "" || spare.ControlToken == "" {
		return hostHandle{}, fmt.Errorf("spare assignment endpoint missing")
	}
	if !modalTunnelRoute.MatchString(spare.ControlRoute) {
		return hostHandle{}, fmt.Errorf("spare assignment requires a Modal HTTPS tunnel")
	}
	body, err := json.Marshal(environment)
	if err != nil {
		return hostHandle{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(spare.ControlRoute, "/")+"/assign", bytes.NewReader(body))
	if err != nil {
		return hostHandle{}, err
	}
	request.Header.Set("Authorization", "Bearer "+spare.ControlToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(request)
	if err != nil {
		return hostHandle{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return hostHandle{}, fmt.Errorf("spare assignment returned HTTP %d", response.StatusCode)
	}
	document, err := io.ReadAll(io.LimitReader(response.Body, maximumCommandBytes+1))
	if err != nil {
		return hostHandle{}, err
	}
	if len(document) > maximumCommandBytes {
		return hostHandle{}, fmt.Errorf("host readiness is too large")
	}
	var handle hostHandle
	err = json.Unmarshal(document, &handle)
	return handle, err
}
