package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("canner", "err", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: canner {serve|deliver|packages} <config.json>")
	}
	switch args[0] {
	case "serve":
		if len(args) != 2 {
			return fmt.Errorf("usage: canner serve <config.json>")
		}
		cfg, err := loadConfig(args[1])
		if err != nil {
			return err
		}
		server, err := newServer(cfg)
		if err != nil {
			return err
		}
		server.startPartialUploadCleanup()
		defer server.close()
		slog.Info("listening", "addr", cfg.ListenAddr, "issuer", cfg.Issuer)
		return http.ListenAndServe(cfg.ListenAddr, server.handler)
	case "deliver":
		if len(args) != 2 {
			return fmt.Errorf("usage: canner deliver <config.json>")
		}
		cfg, err := loadConfig(args[1])
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return runDelivery(ctx, cfg)
	case "packages":
		if len(args) != 2 {
			return fmt.Errorf("usage: canner packages <config.json>")
		}
		cfg, err := loadConfig(args[1])
		if err != nil {
			return err
		}
		return printPackages(context.Background(), cfg, os.Stdout)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
