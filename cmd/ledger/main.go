package main

import (
	"context"
	"log"
	"os"

	"github.com/zzbaichuan-glitch/PayFlow-Lab/internal/distributed"
	"github.com/zzbaichuan-glitch/PayFlow-Lab/internal/participant"
	"github.com/zzbaichuan-glitch/PayFlow-Lab/internal/serverutil"
)

func main() {
	address := envOr("PAYFLOW_PARTICIPANT_ADDR", ":8083")
	db, err := distributed.OpenMySQL(context.Background(), os.Getenv("PAYFLOW_MYSQL_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := serverutil.Run("payflow-ledger", address, participant.NewLedgerHandler(db)); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
