package distributed

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"payflow-lab/internal/domain"
	"payflow-lab/internal/store"
)

type ReservationView struct {
	GID         string    `json:"gid"`
	PaymentID   string    `json:"payment_id"`
	AccountID   string    `json:"account_id"`
	AmountCents int64     `json:"amount_cents"`
	Status      string    `json:"status"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type LedgerView struct {
	GID         string    `json:"gid"`
	PaymentID   string    `json:"payment_id"`
	AccountID   string    `json:"account_id"`
	AmountCents int64     `json:"amount_cents"`
	Status      string    `json:"status"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type BarrierView struct {
	BranchID  string    `json:"branch_id"`
	Op        string    `json:"op"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"created_at"`
}

type TransactionView struct {
	GID               string           `json:"gid"`
	DTMStatus         string           `json:"dtm_status"`
	DTMRollbackReason string           `json:"dtm_rollback_reason,omitempty"`
	Payment           domain.Payment   `json:"payment"`
	Reservation       *ReservationView `json:"account_reservation,omitempty"`
	Ledger            *LedgerView      `json:"ledger_intent,omitempty"`
	Barriers          []BarrierView    `json:"barriers"`
}

func (r *Repository) VerifyCachedIdempotency(ctx context.Context, paymentID, key, fingerprint string) (domain.Payment, bool) {
	row := r.db.QueryRowContext(ctx, paymentSelect+` WHERE id=? AND idempotency_key=? AND request_fingerprint=?`,
		paymentID, key, fingerprint)
	payment, _, err := scanPayment(row)
	return payment, err == nil
}

func (r *Repository) ParticipantStates(ctx context.Context, gid, paymentID string) (reservationStatus, ledgerStatus, dtmStatus string, err error) {
	err = r.db.QueryRowContext(ctx, `
		SELECT status FROM account_reservations WHERE gid=? AND payment_id=?`, gid, paymentID).Scan(&reservationStatus)
	if errors.Is(err, sql.ErrNoRows) {
		reservationStatus = ""
		err = nil
	}
	if err != nil {
		return
	}
	err = r.db.QueryRowContext(ctx, `
		SELECT status FROM ledger_entries WHERE gid=? AND payment_id=?`, gid, paymentID).Scan(&ledgerStatus)
	if errors.Is(err, sql.ErrNoRows) {
		ledgerStatus = ""
		err = nil
	}
	if err != nil {
		return
	}
	err = r.db.QueryRowContext(ctx, `SELECT status FROM dtm.trans_global WHERE gid=?`, gid).Scan(&dtmStatus)
	if errors.Is(err, sql.ErrNoRows) {
		dtmStatus = ""
		err = nil
	}
	return
}

func (r *Repository) Transaction(ctx context.Context, gid string) (TransactionView, error) {
	var view TransactionView
	view.GID = gid
	row := r.db.QueryRowContext(ctx, paymentSelect+` WHERE gid=?`, gid)
	payment, _, err := scanPayment(row)
	if err != nil {
		return view, mapSQLError(err)
	}
	view.Payment = payment
	if err := r.db.QueryRowContext(ctx, `
		SELECT status, rollback_reason FROM dtm.trans_global WHERE gid=?`, gid).
		Scan(&view.DTMStatus, &view.DTMRollbackReason); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return view, err
	}
	var reservation ReservationView
	if err := r.db.QueryRowContext(ctx, `
		SELECT gid, payment_id, account_id, amount_cents, status, updated_at
		FROM account_reservations WHERE gid=?`, gid).
		Scan(&reservation.GID, &reservation.PaymentID, &reservation.AccountID, &reservation.AmountCents,
			&reservation.Status, &reservation.UpdatedAt); err == nil {
		view.Reservation = &reservation
	} else if !errors.Is(err, sql.ErrNoRows) {
		return view, err
	}
	var ledger LedgerView
	if err := r.db.QueryRowContext(ctx, `
		SELECT gid, payment_id, account_id, amount_cents, status, updated_at
		FROM ledger_entries WHERE gid=?`, gid).
		Scan(&ledger.GID, &ledger.PaymentID, &ledger.AccountID, &ledger.AmountCents,
			&ledger.Status, &ledger.UpdatedAt); err == nil {
		view.Ledger = &ledger
	} else if !errors.Is(err, sql.ErrNoRows) {
		return view, err
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT branch_id, op, reason, create_time FROM dtm_barrier.barrier
		WHERE gid=? ORDER BY id`, gid)
	if err != nil {
		return view, err
	}
	defer rows.Close()
	view.Barriers = make([]BarrierView, 0)
	for rows.Next() {
		var barrier BarrierView
		if err := rows.Scan(&barrier.BranchID, &barrier.Op, &barrier.Reason, &barrier.CreatedAt); err != nil {
			return view, err
		}
		view.Barriers = append(view.Barriers, barrier)
	}
	return view, rows.Err()
}

func (r *Repository) PaymentByGID(ctx context.Context, gid string) (domain.Payment, error) {
	payment, _, err := scanPayment(r.db.QueryRowContext(ctx, paymentSelect+` WHERE gid=?`, gid))
	return payment, mapSQLError(err)
}

func (r *Repository) Ping(ctx context.Context) error { return r.db.PingContext(ctx) }

func (r *Repository) EnsureNoActiveTransactions(ctx context.Context) error {
	var count int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM payments WHERE status='PROCESSING'`).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return store.ErrInvalidTransition
	}
	return nil
}
