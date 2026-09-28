package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	socket := flag.String("socket", "", "Serve provider commands over a private Unix socket")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *socket != "" {
		// The parent holds stdin open for the lifetime of the worker.
		go func() { _, _ = io.Copy(io.Discard, os.Stdin); stop() }()
		if err := serveProvider(ctx, *socket, os.Stdout, newModalAPI, time.Now); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := runCommand(ctx, os.Stdin, os.Stdout, newModalAPI, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
