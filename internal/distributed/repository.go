package distributed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"payflow-lab/internal/domain"
	"payflow-lab/internal/store"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func OpenMySQL(ctx context.Context, dsn string) (*sql.DB, error) {
	if dsn == "" {
		return nil, errors.New("PAYFLOW_MYSQL_DSN is required in distributed mode")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(30)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return db, nil
}

func (r *Repository) DB() *sql.DB { return r.db }

func (r *Repository) CreatePayment(ctx context.Context, payment domain.Payment, idempotencyKey, fingerprint string) (domain.Payment, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Payment{}, false, err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO payments
		(id, gid, execution_mode, order_id, account_id, amount_cents, currency, status,
		 failure_reason, last_callback_sequence, idempotency_key, request_fingerprint, created_at, updated_at)
		VALUES (?, NULL, 'distributed', ?, ?, ?, ?, 'INIT', '', 0, ?, ?, ?, ?)`,
		payment.ID, payment.OrderID, payment.AccountID, payment.AmountCents, payment.Currency,
		idempotencyKey, fingerprint, payment.CreatedAt, payment.UpdatedAt)
	if err != nil {
		var mysqlErr *gomysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			_ = tx.Rollback()
			existing, existingFingerprint, findErr := r.findByIdempotencyKey(ctx, idempotencyKey)
			if findErr != nil {
				return domain.Payment{}, false, findErr
			}
			if existingFingerprint != fingerprint {
				return domain.Payment{}, false, store.ErrIdempotencyConflict
			}
			return existing, false, nil
		}
		return domain.Payment{}, false, err
	}
	if err := appendEventTx(ctx, tx, payment.ID, "PAYMENT_CREATED", "", domain.PaymentStatusInit,
		"distributed payment persisted with a MySQL idempotency key", nil); err != nil {
		return domain.Payment{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Payment{}, false, err
	}
	payment.ExecutionMode = "distributed"
	return payment, true, nil
}

func (r *Repository) BeginTCC(ctx context.Context, paymentID, gid string) (domain.Payment, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Payment{}, err
	}
	defer tx.Rollback()

	payment, err := getPaymentTxForUpdate(ctx, tx, paymentID)
	if err != nil {
		return domain.Payment{}, err
	}
	if payment.Status != domain.PaymentStatusInit {
		return domain.Payment{}, store.ErrInvalidTransition
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE payments SET gid=?, status='PROCESSING', updated_at=UTC_TIMESTAMP(6)
		WHERE id=? AND status='INIT'`, gid, paymentID)
	if err != nil {
		return domain.Payment{}, err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return domain.Payment{}, store.ErrInvalidTransition
	}
	if err := appendEventTx(ctx, tx, paymentID, "DTM_TCC_STARTED", domain.PaymentStatusInit,
		domain.PaymentStatusProcessing, "DTM global TCC transaction prepared", map[string]string{"gid": gid}); err != nil {
		return domain.Payment{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Payment{}, err
	}
	return r.GetPayment(ctx, paymentID)
}

func (r *Repository) Finalize(ctx context.Context, paymentID string, status domain.PaymentStatus, reason, message string) (domain.Payment, error) {
	if status != domain.PaymentStatusSuccess && status != domain.PaymentStatusFailed && status != domain.PaymentStatusClosed {
		return domain.Payment{}, fmt.Errorf("unsupported final status %s", status)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Payment{}, err
	}
	defer tx.Rollback()
	payment, err := getPaymentTxForUpdate(ctx, tx, paymentID)
	if err != nil {
		return domain.Payment{}, err
	}
	if payment.Status.Terminal() {
		if payment.Status == status {
			return payment, tx.Commit()
		}
		return domain.Payment{}, store.ErrInvalidTransition
	}
	if payment.Status != domain.PaymentStatusProcessing {
		return domain.Payment{}, store.ErrInvalidTransition
	}
	from := payment.Status
	_, err = tx.ExecContext(ctx, `
		UPDATE payments SET status=?, failure_reason=?, updated_at=UTC_TIMESTAMP(6)
		WHERE id=? AND status='PROCESSING'`, status, reason, paymentID)
	if err != nil {
		return domain.Payment{}, err
	}
	if message == "" {
		message = "DTM TCC transaction finalized"
	}
	if err := appendEventTx(ctx, tx, paymentID, "STATUS_CHANGED", from, status, message, nil); err != nil {
		return domain.Payment{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Payment{}, err
	}
	return r.GetPayment(ctx, paymentID)
}

func (r *Repository) RecordEvent(ctx context.Context, paymentID, eventType, message string, metadata map[string]string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := getPaymentTxForUpdate(ctx, tx, paymentID); err != nil {
		return err
	}
	if err := appendEventTx(ctx, tx, paymentID, eventType, "", "", message, metadata); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *Repository) GetPayment(ctx context.Context, paymentID string) (domain.Payment, error) {
	row := r.db.QueryRowContext(ctx, paymentSelect+` WHERE id=?`, paymentID)
	payment, _, err := scanPayment(row)
	return payment, mapSQLError(err)
}

func (r *Repository) findByIdempotencyKey(ctx context.Context, key string) (domain.Payment, string, error) {
	row := r.db.QueryRowContext(ctx, paymentSelect+` WHERE idempotency_key=?`, key)
	payment, fingerprint, err := scanPayment(row)
	return payment, fingerprint, mapSQLError(err)
}

func (r *Repository) FindByIdempotencyKey(ctx context.Context, key string) (domain.Payment, string, error) {
	return r.findByIdempotencyKey(ctx, key)
}

func (r *Repository) ListPayments(ctx context.Context) ([]domain.Payment, error) {
	rows, err := r.db.QueryContext(ctx, paymentSelect+` ORDER BY created_at DESC LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	payments := make([]domain.Payment, 0)
	for rows.Next() {
		payment, _, err := scanPayment(rows)
		if err != nil {
			return nil, err
		}
		payments = append(payments, payment)
	}
	return payments, rows.Err()
}

func (r *Repository) Events(ctx context.Context, paymentID string) ([]domain.PaymentEvent, error) {
	if _, err := r.GetPayment(ctx, paymentID); err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT sequence, event_type, from_status, to_status, message, metadata, created_at
		FROM payment_events WHERE payment_id=? ORDER BY sequence`, paymentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]domain.PaymentEvent, 0)
	for rows.Next() {
		var event domain.PaymentEvent
		var from, to string
		var rawMetadata []byte
		if err := rows.Scan(&event.Sequence, &event.Type, &from, &to, &event.Message, &rawMetadata, &event.At); err != nil {
			return nil, err
		}
		event.From = domain.PaymentStatus(from)
		event.To = domain.PaymentStatus(to)
		if len(rawMetadata) > 0 {
			_ = json.Unmarshal(rawMetadata, &event.Metadata)
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func (r *Repository) Account(ctx context.Context, accountID string) (domain.Account, error) {
	var account domain.Account
	err := r.db.QueryRowContext(ctx, `
		SELECT id, available_cents, frozen_cents, currency, updated_at FROM accounts WHERE id=?`, accountID).
		Scan(&account.ID, &account.AvailableCents, &account.FrozenCents, &account.Currency, &account.UpdatedAt)
	return account, mapSQLError(err)
}

func (r *Repository) Ledger(ctx context.Context) ([]domain.LedgerEntry, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT CAST(id AS CHAR), gid, payment_id, account_id, amount_cents, direction, status, created_at
		FROM ledger_entries WHERE status='POSTED' ORDER BY id DESC LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := make([]domain.LedgerEntry, 0)
	for rows.Next() {
		var entry domain.LedgerEntry
		if err := rows.Scan(&entry.ID, &entry.GID, &entry.PaymentID, &entry.AccountID, &entry.AmountCents,
			&entry.Direction, &entry.Status, &entry.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func (r *Repository) Snapshot(ctx context.Context) (domain.StoreSnapshot, error) {
	result := domain.StoreSnapshot{StatusCounts: make(map[domain.PaymentStatus]int)}
	rows, err := r.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM payments GROUP BY status`)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var status domain.PaymentStatus
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return result, err
		}
		result.StatusCounts[status] = count
		result.PaymentCount += count
	}
	if err := rows.Close(); err != nil {
		return result, err
	}
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&result.AccountCount); err != nil {
		return result, err
	}
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_entries WHERE status='POSTED'`).Scan(&result.LedgerCount); err != nil {
		return result, err
	}
	return result, nil
}

func (r *Repository) Reset(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	statements := []string{
		`DELETE FROM payment_events`,
		`DELETE FROM ledger_entries`,
		`DELETE FROM account_reservations`,
		`DELETE FROM dtm_barrier.barrier WHERE gid IN (SELECT gid FROM payflow.payments WHERE gid IS NOT NULL)`,
		`DELETE FROM dtm.trans_branch_op WHERE gid IN (SELECT gid FROM payflow.payments WHERE gid IS NOT NULL)`,
		`DELETE FROM dtm.trans_global WHERE gid IN (SELECT gid FROM payflow.payments WHERE gid IS NOT NULL)`,
		`DELETE FROM payments`,
		`UPDATE accounts SET available_cents=1000000, frozen_cents=0, updated_at=UTC_TIMESTAMP(6) WHERE id='demo-user'`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func appendEventTx(ctx context.Context, tx *sql.Tx, paymentID, eventType string, from, to domain.PaymentStatus, message string, metadata map[string]string) error {
	var next int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(sequence), 0) + 1 FROM payment_events WHERE payment_id=?`, paymentID).Scan(&next); err != nil {
		return err
	}
	rawMetadata, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO payment_events(payment_id, sequence, event_type, from_status, to_status, message, metadata, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6))`, paymentID, next, eventType, from, to, message, rawMetadata)
	return err
}

const paymentSelect = `
	SELECT id, COALESCE(gid,''), execution_mode, order_id, account_id, amount_cents, currency,
	       status, failure_reason, last_callback_sequence, created_at, updated_at, request_fingerprint
	FROM payments`

type scanner interface {
	Scan(dest ...any) error
}

func scanPayment(row scanner) (domain.Payment, string, error) {
	var payment domain.Payment
	var fingerprint string
	err := row.Scan(&payment.ID, &payment.GID, &payment.ExecutionMode, &payment.OrderID, &payment.AccountID,
		&payment.AmountCents, &payment.Currency, &payment.Status, &payment.FailureReason,
		&payment.LastCallbackSequence, &payment.CreatedAt, &payment.UpdatedAt, &fingerprint)
	return payment, fingerprint, err
}

func getPaymentTxForUpdate(ctx context.Context, tx *sql.Tx, paymentID string) (domain.Payment, error) {
	payment, _, err := scanPayment(tx.QueryRowContext(ctx, paymentSelect+` WHERE id=? FOR UPDATE`, paymentID))
	return payment, mapSQLError(err)
}

func mapSQLError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return store.ErrNotFound
	}
	return err
}
