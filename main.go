package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("canner", "err", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: canner {serve <config.json>|hash-token <token>}")
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
		slog.Info("listening", "addr", cfg.ListenAddr, "issuer", cfg.Issuer)
		return http.ListenAndServe(cfg.ListenAddr, server.handler)
	case "hash-token":
		if len(args) != 2 || strings.TrimSpace(args[1]) == "" {
			return fmt.Errorf("usage: canner hash-token <token>")
		}
		sum := sha256.Sum256([]byte(args[1]))
		fmt.Println(hex.EncodeToString(sum[:]))
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
