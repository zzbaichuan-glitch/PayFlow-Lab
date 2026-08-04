package main

import (
	"context"
	"log"
	"os"

	"payflow-lab/internal/distributed"
	"payflow-lab/internal/participant"
	"payflow-lab/internal/serverutil"
)

func main() {
	address := envOr("PAYFLOW_PARTICIPANT_ADDR", ":8082")
	db, err := distributed.OpenMySQL(context.Background(), os.Getenv("PAYFLOW_MYSQL_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := serverutil.Run("payflow-account", address, participant.NewAccountHandler(db)); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
