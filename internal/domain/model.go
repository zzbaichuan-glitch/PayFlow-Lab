package domain

import "time"

type PaymentStatus string

const (
	PaymentStatusInit       PaymentStatus = "INIT"
	PaymentStatusProcessing PaymentStatus = "PROCESSING"
	PaymentStatusSuccess    PaymentStatus = "SUCCESS"
	PaymentStatusFailed     PaymentStatus = "FAILED"
	PaymentStatusClosed     PaymentStatus = "CLOSED"
)

func (s PaymentStatus) Terminal() bool {
	return s == PaymentStatusSuccess || s == PaymentStatusFailed || s == PaymentStatusClosed
}

type Payment struct {
	ID                   string        `json:"id"`
	GID                  string        `json:"gid,omitempty"`
	ExecutionMode        string        `json:"execution_mode,omitempty"`
	OrderID              string        `json:"order_id"`
	AccountID            string        `json:"account_id"`
	AmountCents          int64         `json:"amount_cents"`
	Currency             string        `json:"currency"`
	Status               PaymentStatus `json:"status"`
	FailureReason        string        `json:"failure_reason,omitempty"`
	LastCallbackSequence int64         `json:"last_callback_sequence"`
	CreatedAt            time.Time     `json:"created_at"`
	UpdatedAt            time.Time     `json:"updated_at"`
}

type PaymentEvent struct {
	Sequence int64             `json:"sequence"`
	Type     string            `json:"type"`
	From     PaymentStatus     `json:"from,omitempty"`
	To       PaymentStatus     `json:"to,omitempty"`
	Message  string            `json:"message"`
	Metadata map[string]string `json:"metadata,omitempty"`
	At       time.Time         `json:"at"`
}

type Callback struct {
	EventID  string `json:"event_id"`
	Sequence int64  `json:"sequence"`
	Outcome  string `json:"outcome"`
	Reason   string `json:"reason,omitempty"`
}

type CallbackDisposition string

const (
	CallbackApplied         CallbackDisposition = "applied"
	CallbackDuplicate       CallbackDisposition = "duplicate"
	CallbackOutOfOrder      CallbackDisposition = "out_of_order"
	CallbackTerminalIgnored CallbackDisposition = "terminal_ignored"
)

type CallbackResult struct {
	Payment     Payment             `json:"payment"`
	Disposition CallbackDisposition `json:"disposition"`
}

type Account struct {
	ID             string    `json:"id"`
	AvailableCents int64     `json:"available_cents"`
	FrozenCents    int64     `json:"frozen_cents"`
	Currency       string    `json:"currency"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type LedgerEntry struct {
	ID          string    `json:"id"`
	GID         string    `json:"gid,omitempty"`
	PaymentID   string    `json:"payment_id"`
	AccountID   string    `json:"account_id"`
	AmountCents int64     `json:"amount_cents"`
	Direction   string    `json:"direction"`
	Status      string    `json:"status,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type StoreSnapshot struct {
	PaymentCount int                   `json:"payment_count"`
	StatusCounts map[PaymentStatus]int `json:"status_counts"`
	AccountCount int                   `json:"account_count"`
	LedgerCount  int                   `json:"ledger_count"`
}
