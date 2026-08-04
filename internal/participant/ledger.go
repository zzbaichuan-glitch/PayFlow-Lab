package participant

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/dtm-labs/dtm/client/dtmcli"
)

func NewLedgerHandler(db *sql.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler("payflow-ledger", db))
	mux.HandleFunc("POST /internal/tcc/ledger/try", branchHandler(db, "try", ledgerTry))
	mux.HandleFunc("POST /internal/tcc/ledger/confirm", branchHandler(db, "confirm", ledgerConfirm))
	mux.HandleFunc("POST /internal/tcc/ledger/cancel", branchHandler(db, "cancel", ledgerCancel))
	return mux
}

func ledgerTry(tx *sql.Tx, request TCCRequest) error {
	if request.FailTry {
		return dtmcli.ErrFailure
	}
	if request.PaymentID == "" || request.GID == "" || request.AccountID == "" || request.AmountCents <= 0 {
		return dtmcli.ErrFailure
	}
	_, err := tx.Exec(`
		INSERT INTO ledger_entries(gid, payment_id, account_id, amount_cents, direction, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'DEBIT', 'PREPARED', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))`,
		request.GID, request.PaymentID, request.AccountID, request.AmountCents)
	return err
}

func ledgerConfirm(tx *sql.Tx, request TCCRequest) error {
	result, err := tx.Exec(`
		UPDATE ledger_entries SET status='POSTED', updated_at=UTC_TIMESTAMP(6)
		WHERE gid=? AND payment_id=? AND status='PREPARED'`, request.GID, request.PaymentID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 1 {
		return nil
	}
	var status string
	err = tx.QueryRow(`SELECT status FROM ledger_entries WHERE gid=? AND payment_id=?`, request.GID, request.PaymentID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) || status == "POSTED" || status == "CANCELLED" {
		return nil
	}
	return err
}

func ledgerCancel(tx *sql.Tx, request TCCRequest) error {
	_, err := tx.Exec(`
		UPDATE ledger_entries SET status='CANCELLED', updated_at=UTC_TIMESTAMP(6)
		WHERE gid=? AND payment_id=? AND status='PREPARED'`, request.GID, request.PaymentID)
	return err
}
