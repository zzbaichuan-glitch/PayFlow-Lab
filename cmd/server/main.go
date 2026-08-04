package main

import (
	"context"
	"log"
	"os"
	"strings"

	"payflow-lab/internal/distributed"
	"payflow-lab/internal/httpapi"
	"payflow-lab/internal/serverutil"
	"payflow-lab/internal/service"
	"payflow-lab/internal/store"
)

func main() {
	address := envOr("PAYFLOW_ADDR", "127.0.0.1:8081")
	mode := strings.ToLower(envOr("PAYFLOW_MODE", "memory"))

	var backend httpapi.Backend
	if mode == "distributed" {
		db, err := distributed.OpenMySQL(context.Background(), os.Getenv("PAYFLOW_MYSQL_DSN"))
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		cache, err := distributed.OpenRedis(context.Background(), os.Getenv("PAYFLOW_REDIS_ADDR"),
			os.Getenv("PAYFLOW_REDIS_PASSWORD"), envOr("PAYFLOW_REDIS_PREFIX", "payflow:"))
		if err != nil {
			log.Fatalf("connect redis: %v", err)
		}
		defer cache.Close()
		backend, err = distributed.NewService(distributed.NewRepository(db), cache, distributed.ServiceConfig{
			DTMURL:     envOr("PAYFLOW_DTM_URL", "http://dtm:36789/api/dtmsvr"),
			AccountURL: envOr("PAYFLOW_ACCOUNT_URL", "http://payflow-account:8082"),
			LedgerURL:  envOr("PAYFLOW_LEDGER_URL", "http://payflow-ledger:8083"),
		})
		if err != nil {
			log.Fatal(err)
		}
	} else if mode == "memory" {
		backend = service.NewPaymentService(store.NewMemoryStore())
	} else {
		log.Fatalf("unsupported PAYFLOW_MODE %q (use memory or distributed)", mode)
	}

	log.Printf("PayFlow Lab 0.2.0 mode=%s", mode)
	if err := serverutil.Run("payflow", address, httpapi.NewHandler(backend)); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
