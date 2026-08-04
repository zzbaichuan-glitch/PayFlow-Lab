package distributed

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/go-resty/resty/v2"

	"payflow-lab/internal/domain"
	"payflow-lab/internal/participant"
	"payflow-lab/internal/service"
	"payflow-lab/internal/store"
)

type ServiceConfig struct {
	DTMURL     string
	AccountURL string
	LedgerURL  string
}

type Service struct {
	repository        *Repository
	cache             *Cache
	dtmURL            string
	accountURL        string
	ledgerURL         string
	httpClient        *http.Client
	started           atomic.Int64
	cacheHits         atomic.Uint64
	idempotentReplays atomic.Uint64
	faultsInjected    atomic.Uint64
}

func NewService(repository *Repository, cache *Cache, config ServiceConfig) (*Service, error) {
	config.DTMURL = strings.TrimRight(strings.TrimSpace(config.DTMURL), "/")
	config.AccountURL = strings.TrimRight(strings.TrimSpace(config.AccountURL), "/")
	config.LedgerURL = strings.TrimRight(strings.TrimSpace(config.LedgerURL), "/")
	if config.DTMURL == "" || config.AccountURL == "" || config.LedgerURL == "" {
		return nil, errors.New("DTM, account participant, and ledger participant URLs are required")
	}
	result := &Service{
		repository: repository, cache: cache, dtmURL: config.DTMURL,
		accountURL: config.AccountURL, ledgerURL: config.LedgerURL,
		httpClient: &http.Client{Timeout: 3 * time.Second},
	}
	result.started.Store(time.Now().Unix())
	return result, nil
}

func (s *Service) Mode() string { return "distributed" }

func (s *Service) Create(ctx context.Context, input service.CreatePaymentInput) (service.CreatePaymentResult, error) {
	input = service.NormalizeCreateInput(input)
	fault := input.Fault
	input.Fault = ""
	if err := service.ValidateCreateInput(input); err != nil {
		return service.CreatePaymentResult{}, err
	}
	if fault != "" && fault != "after_account_try" && fault != "ledger_try" {
		return service.CreatePaymentResult{}, &service.ValidationError{Message: "distributed fault must be after_account_try or ledger_try"}
	}
	if fault != "" {
		s.faultsInjected.Add(1)
	}
	fingerprint := service.Fingerprint(input)

	if paymentID, cachedFingerprint, ok := s.cache.GetIdempotency(ctx, input.IdempotencyKey); ok && cachedFingerprint == fingerprint {
		if payment, verified := s.repository.VerifyCachedIdempotency(ctx, paymentID, input.IdempotencyKey, fingerprint); verified {
			s.cacheHits.Add(1)
			s.idempotentReplays.Add(1)
			payment = s.reconcile(ctx, payment)
			return service.CreatePaymentResult{Payment: payment, IdempotentReplay: true}, nil
		}
	}
	if existing, existingFingerprint, err := s.repository.FindByIdempotencyKey(ctx, input.IdempotencyKey); err == nil {
		if existingFingerprint != fingerprint {
			return service.CreatePaymentResult{}, store.ErrIdempotencyConflict
		}
		s.cache.SetIdempotency(ctx, input.IdempotencyKey, existing.ID, fingerprint)
		s.idempotentReplays.Add(1)
		existing = s.reconcile(ctx, existing)
		return service.CreatePaymentResult{Payment: existing, IdempotentReplay: true}, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return service.CreatePaymentResult{}, err
	}

	gid, err := safeGenerateGID(s.dtmURL)
	if err != nil {
		return service.CreatePaymentResult{}, fmt.Errorf("generate DTM gid: %w", err)
	}
	now := time.Now().UTC()
	payment := domain.Payment{
		ID: service.NewID("pay"), GID: gid, ExecutionMode: "distributed",
		OrderID: input.OrderID, AccountID: input.AccountID, AmountCents: input.AmountCents,
		Currency: input.Currency, Status: domain.PaymentStatusInit, CreatedAt: now, UpdatedAt: now,
	}
	payment, created, err := s.repository.CreatePayment(ctx, payment, input.IdempotencyKey, fingerprint)
	if err != nil {
		return service.CreatePaymentResult{}, err
	}
	if !created {
		s.cache.SetIdempotency(ctx, input.IdempotencyKey, payment.ID, fingerprint)
		s.idempotentReplays.Add(1)
		return service.CreatePaymentResult{Payment: s.reconcile(ctx, payment), IdempotentReplay: true}, nil
	}
	payment, err = s.repository.BeginTCC(ctx, payment.ID, gid)
	if err != nil {
		return service.CreatePaymentResult{}, err
	}
	s.cache.SetIdempotency(ctx, input.IdempotencyKey, payment.ID, fingerprint)

	request := participant.TCCRequest{
		PaymentID: payment.ID, GID: gid, AccountID: payment.AccountID, AmountCents: payment.AmountCents,
	}
	tccErr := dtmcli.TccGlobalTransaction2(s.dtmURL, gid, func(tcc *dtmcli.Tcc) {
		tcc.Context = ctx
		tcc.WaitResult = true
		tcc.RequestTimeout = 5
		tcc.TimeoutToFail = 15
		tcc.RetryInterval = 10
	}, func(tcc *dtmcli.Tcc) (*resty.Response, error) {
		response, branchErr := tcc.CallBranch(request,
			s.accountURL+"/internal/tcc/account/try",
			s.accountURL+"/internal/tcc/account/confirm",
			s.accountURL+"/internal/tcc/account/cancel")
		if branchErr != nil {
			return response, branchErr
		}
		if fault == "after_account_try" {
			return response, dtmcli.ErrFailure
		}
		ledgerRequest := request
		ledgerRequest.FailTry = fault == "ledger_try"
		return tcc.CallBranch(ledgerRequest,
			s.ledgerURL+"/internal/tcc/ledger/try",
			s.ledgerURL+"/internal/tcc/ledger/confirm",
			s.ledgerURL+"/internal/tcc/ledger/cancel")
	})

	if tccErr != nil {
		_ = s.repository.RecordEvent(ctx, payment.ID, "DTM_TCC_ABORTED", "DTM aborted the global transaction and requested Cancel", map[string]string{
			"gid": gid, "reason": truncate(tccErr.Error(), 300), "fault": fault,
		})
		if s.waitForOutcome(ctx, payment.ID, gid, false, 8*time.Second) {
			payment, err = s.repository.Finalize(ctx, payment.ID, domain.PaymentStatusFailed,
				truncate("DTM TCC cancelled: "+tccErr.Error(), 500), "DTM TCC Cancel completed; no POSTED ledger entry is valid")
			if err != nil && !errors.Is(err, store.ErrInvalidTransition) {
				return service.CreatePaymentResult{}, err
			}
			if err != nil {
				payment, _ = s.repository.GetPayment(ctx, payment.ID)
			}
		} else {
			_ = s.repository.RecordEvent(ctx, payment.ID, "DTM_RECONCILE_PENDING",
				"DTM abort was accepted, but Cancel completion is not yet proven; payment remains PROCESSING",
				map[string]string{"gid": gid})
			payment, _ = s.repository.GetPayment(ctx, payment.ID)
		}
		return service.CreatePaymentResult{Payment: payment}, nil
	}

	if s.waitForOutcome(ctx, payment.ID, gid, true, 8*time.Second) {
		payment, err = s.repository.Finalize(ctx, payment.ID, domain.PaymentStatusSuccess, "",
			"DTM confirmed account and ledger branches through BranchBarrier")
		if err != nil && !errors.Is(err, store.ErrInvalidTransition) {
			return service.CreatePaymentResult{}, err
		}
		if err != nil {
			payment, _ = s.repository.GetPayment(ctx, payment.ID)
		}
	} else {
		_ = s.repository.RecordEvent(ctx, payment.ID, "DTM_RECONCILE_PENDING",
			"DTM accepted submit; participant confirmation is still being reconciled", map[string]string{"gid": gid})
		payment, _ = s.repository.GetPayment(ctx, payment.ID)
	}
	return service.CreatePaymentResult{Payment: payment}, nil
}

func (s *Service) Callback(ctx context.Context, paymentID string, input service.CallbackInput) (domain.CallbackResult, error) {
	if strings.TrimSpace(input.EventID) == "" || input.Sequence <= 0 {
		return domain.CallbackResult{}, &service.ValidationError{Message: "event_id and a positive sequence are required"}
	}
	payment, err := s.repository.GetPayment(ctx, strings.TrimSpace(paymentID))
	if err != nil {
		return domain.CallbackResult{}, err
	}
	payment = s.reconcile(ctx, payment)
	if payment.Status.Terminal() {
		_ = s.repository.RecordEvent(ctx, payment.ID, "CALLBACK_TERMINAL_IGNORED",
			"external callback ignored because DTM payment is terminal", map[string]string{
				"event_id": input.EventID, "callback_sequence": fmt.Sprint(input.Sequence), "outcome": input.Outcome,
			})
		return domain.CallbackResult{Payment: payment, Disposition: domain.CallbackTerminalIgnored}, nil
	}
	return domain.CallbackResult{}, store.ErrInvalidTransition
}

func (s *Service) Close(context.Context, string, string) (domain.Payment, error) {
	return domain.Payment{}, store.ErrInvalidTransition
}

func (s *Service) Details(ctx context.Context, paymentID string) (service.PaymentDetails, error) {
	payment, err := s.repository.GetPayment(ctx, strings.TrimSpace(paymentID))
	if err != nil {
		return service.PaymentDetails{}, err
	}
	payment = s.reconcile(ctx, payment)
	events, err := s.repository.Events(ctx, payment.ID)
	if err != nil {
		return service.PaymentDetails{}, err
	}
	return service.PaymentDetails{Payment: payment, Events: events}, nil
}

func (s *Service) Events(ctx context.Context, paymentID string) ([]domain.PaymentEvent, error) {
	return s.repository.Events(ctx, strings.TrimSpace(paymentID))
}

func (s *Service) List(ctx context.Context) ([]domain.Payment, error) {
	payments, err := s.repository.ListPayments(ctx)
	if err != nil {
		return nil, err
	}
	for index := range payments {
		payments[index] = s.reconcile(ctx, payments[index])
	}
	return payments, nil
}

func (s *Service) Account(ctx context.Context, accountID string) (domain.Account, error) {
	return s.repository.Account(ctx, strings.TrimSpace(accountID))
}

func (s *Service) Ledger(ctx context.Context) ([]domain.LedgerEntry, error) {
	return s.repository.Ledger(ctx)
}

func (s *Service) Metrics(ctx context.Context) (service.MetricsSnapshot, error) {
	snapshot, err := s.repository.Snapshot(ctx)
	if err != nil {
		return service.MetricsSnapshot{}, err
	}
	var dtmCount uint64
	_ = s.repository.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM dtm.trans_global`).Scan(&dtmCount)
	return service.MetricsSnapshot{
		Mode: "distributed", UptimeSeconds: time.Now().Unix() - s.started.Load(),
		PaymentsTotal: snapshot.PaymentCount, PaymentsByStatus: snapshot.StatusCounts,
		AccountsTotal: snapshot.AccountCount, LedgerEntries: snapshot.LedgerCount,
		PaymentsCreatedTotal: uint64(snapshot.PaymentCount), RedisIdempotencyHitsTotal: s.cacheHits.Load(),
		IdempotentReplaysTotal: s.idempotentReplays.Load(), FaultsInjectedTotal: s.faultsInjected.Load(),
		DTMTransactionsTotal: dtmCount,
	}, nil
}

func (s *Service) Reset(ctx context.Context) error {
	if err := s.repository.EnsureNoActiveTransactions(ctx); err != nil {
		return err
	}
	if err := s.repository.Reset(ctx); err != nil {
		return err
	}
	if err := s.cache.Clear(ctx); err != nil {
		return err
	}
	s.cacheHits.Store(0)
	s.idempotentReplays.Store(0)
	s.faultsInjected.Store(0)
	s.started.Store(time.Now().Unix())
	return nil
}

func (s *Service) Transaction(ctx context.Context, gid string) (TransactionView, error) {
	payment, err := s.repository.PaymentByGID(ctx, strings.TrimSpace(gid))
	if err != nil {
		return TransactionView{}, err
	}
	s.reconcile(ctx, payment)
	return s.repository.Transaction(ctx, gid)
}

func (s *Service) Health(ctx context.Context) (map[string]any, error) {
	dependencies := map[string]string{"mysql": "ok", "redis": "ok", "dtm": "ok", "account": "ok", "ledger": "ok"}
	var failures []string
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.repository.Ping(checkCtx); err != nil {
		dependencies["mysql"] = "error"
		failures = append(failures, "mysql")
	}
	if err := s.cache.Ping(checkCtx); err != nil {
		dependencies["redis"] = "error"
		failures = append(failures, "redis")
	}
	checks := map[string]string{
		"dtm": s.dtmURL + "/newGid", "account": s.accountURL + "/healthz", "ledger": s.ledgerURL + "/healthz",
	}
	for name, url := range checks {
		request, _ := http.NewRequestWithContext(checkCtx, http.MethodGet, url, nil)
		response, err := s.httpClient.Do(request)
		if err != nil || response.StatusCode != http.StatusOK {
			dependencies[name] = "error"
			failures = append(failures, name)
		}
		if response != nil {
			response.Body.Close()
		}
	}
	data := map[string]any{"status": "ok", "service": "payflow-lab", "version": "0.2.0", "mode": "distributed", "dependencies": dependencies, "time": time.Now().UTC()}
	if len(failures) > 0 {
		data["status"] = "degraded"
		return data, fmt.Errorf("unhealthy dependencies: %s", strings.Join(failures, ","))
	}
	return data, nil
}

func (s *Service) reconcile(ctx context.Context, payment domain.Payment) domain.Payment {
	if payment.Status != domain.PaymentStatusProcessing || payment.GID == "" {
		return payment
	}
	reservation, ledger, dtmStatus, err := s.repository.ParticipantStates(ctx, payment.GID, payment.ID)
	if err != nil {
		return payment
	}
	if reservation == "CONFIRMED" && ledger == "POSTED" && dtmStatus == "succeed" {
		if updated, err := s.repository.Finalize(ctx, payment.ID, domain.PaymentStatusSuccess, "", "reconciler observed all DTM Confirm branches"); err == nil {
			return updated
		}
	}
	if dtmStatus == "failed" && reservation != "FROZEN" && ledger != "POSTED" {
		if updated, err := s.repository.Finalize(ctx, payment.ID, domain.PaymentStatusFailed,
			"DTM global transaction failed", "reconciler observed DTM Cancel completion"); err == nil {
			return updated
		}
	}
	return payment
}

func (s *Service) waitForOutcome(ctx context.Context, paymentID, gid string, success bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		reservation, ledger, dtmStatus, err := s.repository.ParticipantStates(ctx, gid, paymentID)
		if err == nil {
			if success && reservation == "CONFIRMED" && ledger == "POSTED" && dtmStatus == "succeed" {
				return true
			}
			if !success && dtmStatus == "failed" && reservation != "FROZEN" && ledger != "POSTED" {
				return true
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	return false
}

func safeGenerateGID(dtmURL string) (gid string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%v", recovered)
		}
	}()
	return dtmcli.MustGenGid(dtmURL), nil
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
