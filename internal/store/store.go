package store

import (
	"context"
	"errors"

	"github.com/zzbaichuan-glitch/PayFlow-Lab/internal/domain"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrIdempotencyConflict = errors.New("idempotency key was already used with a different request")
	ErrInvalidTransition   = errors.New("invalid payment state transition")
	ErrInsufficientFunds   = errors.New("insufficient funds")
)

// Store describes the durable operations required by the payment service.
// The memory implementation is the default learning adapter. A database adapter
// can implement this interface without changing HTTP or orchestration code.
type Store interface {
	CreatePayment(ctx context.Context, payment domain.Payment, idempotencyKey, fingerprint string) (domain.Payment, bool, error)
	StartPayment(ctx context.Context, paymentID string) (domain.Payment, error)
	Freeze(ctx context.Context, paymentID string) (domain.Payment, error)
	FailAndRelease(ctx context.Context, paymentID, reason string) (domain.Payment, error)
	ProcessCallback(ctx context.Context, paymentID string, callback domain.Callback) (domain.CallbackResult, error)
	ClosePayment(ctx context.Context, paymentID, reason string) (domain.Payment, error)
	RecordEvent(ctx context.Context, paymentID, eventType, message string, metadata map[string]string) error

	GetPayment(ctx context.Context, paymentID string) (domain.Payment, error)
	ListPayments(ctx context.Context) ([]domain.Payment, error)
	Events(ctx context.Context, paymentID string) ([]domain.PaymentEvent, error)
	Account(ctx context.Context, accountID string) (domain.Account, error)
	Ledger(ctx context.Context) ([]domain.LedgerEntry, error)
	Snapshot(ctx context.Context) (domain.StoreSnapshot, error)
	Reset(ctx context.Context) error
}
