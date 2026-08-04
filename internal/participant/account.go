package participant

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/dtm-labs/dtm/client/dtmcli"
)

func NewAccountHandler(db *sql.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler("payflow-account", db))
	mux.HandleFunc("POST /internal/tcc/account/try", branchHandler(db, "try", accountTry))
	mux.HandleFunc("POST /internal/tcc/account/confirm", branchHandler(db, "confirm", accountConfirm))
	mux.HandleFunc("POST /internal/tcc/account/cancel", branchHandler(db, "cancel", accountCancel))
	return mux
}

func accountTry(tx *sql.Tx, request TCCRequest) error {
	if request.PaymentID == "" || request.GID == "" || request.AccountID == "" || request.AmountCents <= 0 {
		return dtmcli.ErrFailure
	}
	var available, frozen int64
	if err := tx.QueryRow(`SELECT available_cents, frozen_cents FROM accounts WHERE id=? FOR UPDATE`, request.AccountID).
		Scan(&available, &frozen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return dtmcli.ErrFailure
		}
		return err
	}
	if available < request.AmountCents {
		return dtmcli.ErrFailure
	}
	if _, err := tx.Exec(`
		INSERT INTO account_reservations(gid, payment_id, account_id, amount_cents, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'FROZEN', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))`,
		request.GID, request.PaymentID, request.AccountID, request.AmountCents); err != nil {
		return err
	}
	result, err := tx.Exec(`
		UPDATE accounts SET available_cents=available_cents-?, frozen_cents=frozen_cents+?, updated_at=UTC_TIMESTAMP(6)
		WHERE id=? AND available_cents>=?`, request.AmountCents, request.AmountCents, request.AccountID, request.AmountCents)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return dtmcli.ErrFailure
	}
	return nil
}

func accountConfirm(tx *sql.Tx, request TCCRequest) error {
	var accountID, status string
	var amount int64
	err := tx.QueryRow(`
		SELECT account_id, amount_cents, status FROM account_reservations
		WHERE gid=? AND payment_id=? FOR UPDATE`, request.GID, request.PaymentID).
		Scan(&accountID, &amount, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if status == "CONFIRMED" {
		return nil
	}
	if status != "FROZEN" {
		return nil
	}
	result, err := tx.Exec(`
		UPDATE accounts SET frozen_cents=frozen_cents-?, updated_at=UTC_TIMESTAMP(6)
		WHERE id=? AND frozen_cents>=?`, amount, accountID, amount)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return errors.New("account frozen balance invariant violated")
	}
	_, err = tx.Exec(`
		UPDATE account_reservations SET status='CONFIRMED', updated_at=UTC_TIMESTAMP(6)
		WHERE gid=? AND payment_id=? AND status='FROZEN'`, request.GID, request.PaymentID)
	return err
}

func accountCancel(tx *sql.Tx, request TCCRequest) error {
	var accountID, status string
	var amount int64
	err := tx.QueryRow(`
		SELECT account_id, amount_cents, status FROM account_reservations
		WHERE gid=? AND payment_id=? FOR UPDATE`, request.GID, request.PaymentID).
		Scan(&accountID, &amount, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if status == "CANCELLED" || status != "FROZEN" {
		return nil
	}
	result, err := tx.Exec(`
		UPDATE accounts SET available_cents=available_cents+?, frozen_cents=frozen_cents-?, updated_at=UTC_TIMESTAMP(6)
		WHERE id=? AND frozen_cents>=?`, amount, amount, accountID, amount)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return errors.New("account frozen balance invariant violated")
	}
	_, err = tx.Exec(`
		UPDATE account_reservations SET status='CANCELLED', updated_at=UTC_TIMESTAMP(6)
		WHERE gid=? AND payment_id=? AND status='FROZEN'`, request.GID, request.PaymentID)
	return err
}
