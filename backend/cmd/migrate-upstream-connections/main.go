package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	dbent "github.com/Wei-Shaw/sub2api/ent"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "dry run without mutation")
	ackHash := flag.String("ack-inventory-hash", "", "acknowledge inventory hash for apply")
	flag.Parse()

	if !*dryRun && *ackHash == "" {
		fmt.Fprintln(os.Stderr, "apply requires --ack-inventory-hash")
		os.Exit(1)
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	// Simplified client init – rely on existing DB wiring
	// For Phase 4, this command is reviewed and not auto-run; stub implementation uses ent client via config
	entClient, err := dbent.Open("postgres", cfg.Database.DSN())
	if err != nil {
		fmt.Fprintf(os.Stderr, "open ent: %v\n", err)
		os.Exit(1)
	}
	defer entClient.Close()
	repo := repository.NewUpstreamConnectionMigrationRepository(entClient, nil)
	svc := service.NewUpstreamConnectionMigrationService(repo)
	ctx := context.Background()

	if *dryRun {
		report, err := svc.DryRun(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "dry-run failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("dry-run inventory_hash=%s total=%d to_create=%d unsupported=%v\n", report.InventoryHash, report.TotalAccounts, report.FirstPartyToCreate, report.UnsupportedPlatforms)
		// Secret-redacted report printed
		_ = report
		return
	}
	report, err := svc.Apply(ctx, *ackHash)
	if err != nil {
		fmt.Fprintf(os.Stderr, "apply failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("apply complete inventory_hash=%s created=%d\n", report.InventoryHash, report.FirstPartyToCreate)
}
