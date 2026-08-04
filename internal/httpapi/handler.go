package httpapi

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"payflow-lab/internal/distributed"
	"payflow-lab/internal/domain"
	"payflow-lab/internal/service"
	"payflow-lab/internal/store"
)

//go:embed web/index.html
var webFiles embed.FS

type Handler struct {
	service Backend
}

type Backend interface {
	Create(context.Context, service.CreatePaymentInput) (service.CreatePaymentResult, error)
	Callback(context.Context, string, service.CallbackInput) (domain.CallbackResult, error)
	Close(context.Context, string, string) (domain.Payment, error)
	Details(context.Context, string) (service.PaymentDetails, error)
	Events(context.Context, string) ([]domain.PaymentEvent, error)
	List(context.Context) ([]domain.Payment, error)
	Account(context.Context, string) (domain.Account, error)
	Ledger(context.Context) ([]domain.LedgerEntry, error)
	Metrics(context.Context) (service.MetricsSnapshot, error)
	Reset(context.Context) error
}

func NewHandler(paymentService Backend) http.Handler {
	h := &Handler{service: paymentService}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", h.index)
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("GET /api/v1/payments", h.listPayments)
	mux.HandleFunc("POST /api/v1/payments", h.createPayment)
	mux.HandleFunc("POST /api/v1/distributed/payments", h.createDistributedPayment)
	mux.HandleFunc("GET /api/v1/distributed/transactions/{gid}", h.getDistributedTransaction)
	mux.HandleFunc("GET /api/v1/system", h.systemInfo)
	mux.HandleFunc("GET /api/v1/payments/{id}", h.getPayment)
	mux.HandleFunc("GET /api/v1/payments/{id}/events", h.getEvents)
	mux.HandleFunc("POST /api/v1/payments/{id}/callbacks", h.callback)
	mux.HandleFunc("POST /api/v1/payments/{id}/close", h.closePayment)
	mux.HandleFunc("GET /api/v1/accounts/{id}", h.getAccount)
	mux.HandleFunc("GET /api/v1/ledger", h.getLedger)
	mux.HandleFunc("GET /api/v1/metrics", h.getMetrics)
	mux.HandleFunc("POST /api/v1/demo/reset", h.resetDemo)
	mux.HandleFunc("/", h.notFound)

	return recoverMiddleware(mux)
}

func (h *Handler) notFound(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotFound, "not_found", "route not found")
}

type createPaymentRequest struct {
	OrderID        string `json:"order_id"`
	AccountID      string `json:"account_id"`
	AmountCents    int64  `json:"amount_cents"`
	Currency       string `json:"currency,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	AutoConfirm    bool   `json:"auto_confirm,omitempty"`
	Fault          string `json:"fault,omitempty"`
}

type callbackRequest struct {
	EventID  string `json:"event_id"`
	Sequence int64  `json:"sequence"`
	Outcome  string `json:"outcome"`
	Reason   string `json:"reason,omitempty"`
	Fault    string `json:"fault,omitempty"`
}

type closeRequest struct {
	Reason string `json:"reason,omitempty"`
}

func (h *Handler) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeError(w, http.StatusNotFound, "not_found", "route not found")
		return
	}
	page, err := webFiles.ReadFile("web/index.html")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "demo page unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(page)
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if healthBackend, ok := h.service.(interface {
		Health(context.Context) (map[string]any, error)
	}); ok {
		data, err := healthBackend.Health(r.Context())
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, responseEnvelope{
				Success: false,
				Data:    data,
				Error:   &responseError{Code: "dependency_unavailable", Message: err.Error()},
			})
			return
		}
		writeData(w, http.StatusOK, data)
		return
	}
	writeData(w, http.StatusOK, map[string]any{
		"status": "ok", "service": "payflow-lab", "version": "0.1.0",
		"mode": "memory", "time": time.Now().UTC(),
	})
}

func (h *Handler) createDistributedPayment(w http.ResponseWriter, r *http.Request) {
	if modeBackend, ok := h.service.(interface{ Mode() string }); !ok || modeBackend.Mode() != "distributed" {
		writeError(w, http.StatusConflict, "distributed_mode_required", "start PayFlow with PAYFLOW_MODE=distributed")
		return
	}
	h.createPayment(w, r)
}

func (h *Handler) getDistributedTransaction(w http.ResponseWriter, r *http.Request) {
	inspector, ok := h.service.(interface {
		Transaction(context.Context, string) (distributed.TransactionView, error)
	})
	if !ok {
		writeError(w, http.StatusConflict, "distributed_mode_required", "transaction diagnostics require distributed mode")
		return
	}
	view, err := inspector.Transaction(r.Context(), r.PathValue("gid"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeData(w, http.StatusOK, view)
}

func (h *Handler) systemInfo(w http.ResponseWriter, _ *http.Request) {
	mode := "memory"
	if modeBackend, ok := h.service.(interface{ Mode() string }); ok {
		mode = modeBackend.Mode()
	}
	data := map[string]any{
		"mode":            mode,
		"distributed_tcc": mode == "distributed",
		"fault_points":    []string{},
	}
	if mode == "distributed" {
		data["coordinator"] = map[string]string{"implementation": "DTM", "version": "1.19.0"}
		data["fault_points"] = []string{"after_account_try", "ledger_try"}
	}
	writeData(w, http.StatusOK, data)
}

func (h *Handler) createPayment(w http.ResponseWriter, r *http.Request) {
	var request createPaymentRequest
	if err := decodeJSON(w, r, &request, false); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		idempotencyKey = request.IdempotencyKey
	}
	result, err := h.service.Create(r.Context(), service.CreatePaymentInput{
		OrderID: request.OrderID, AccountID: request.AccountID,
		AmountCents: request.AmountCents, Currency: request.Currency,
		IdempotencyKey: idempotencyKey, AutoConfirm: request.AutoConfirm, Fault: request.Fault,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	status := http.StatusCreated
	if result.IdempotentReplay {
		status = http.StatusOK
	}
	writeData(w, status, result)
}

func (h *Handler) listPayments(w http.ResponseWriter, r *http.Request) {
	payments, err := h.service.List(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeData(w, http.StatusOK, map[string]any{"payments": payments})
}

func (h *Handler) getPayment(w http.ResponseWriter, r *http.Request) {
	details, err := h.service.Details(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeData(w, http.StatusOK, details)
}

func (h *Handler) getEvents(w http.ResponseWriter, r *http.Request) {
	paymentID := r.PathValue("id")
	events, err := h.service.Events(r.Context(), paymentID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeData(w, http.StatusOK, map[string]any{"payment_id": paymentID, "events": events})
}

func (h *Handler) callback(w http.ResponseWriter, r *http.Request) {
	var request callbackRequest
	if err := decodeJSON(w, r, &request, false); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	result, err := h.service.Callback(r.Context(), r.PathValue("id"), service.CallbackInput{
		EventID: request.EventID, Sequence: request.Sequence, Outcome: request.Outcome,
		Reason: request.Reason, Fault: request.Fault,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeData(w, http.StatusOK, result)
}

func (h *Handler) closePayment(w http.ResponseWriter, r *http.Request) {
	var request closeRequest
	if err := decodeJSON(w, r, &request, true); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	payment, err := h.service.Close(r.Context(), r.PathValue("id"), request.Reason)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeData(w, http.StatusOK, map[string]any{"payment": payment})
}

func (h *Handler) getAccount(w http.ResponseWriter, r *http.Request) {
	account, err := h.service.Account(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeData(w, http.StatusOK, map[string]any{"account": account})
}

func (h *Handler) getLedger(w http.ResponseWriter, r *http.Request) {
	entries, err := h.service.Ledger(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeData(w, http.StatusOK, map[string]any{"entries": entries})
}

func (h *Handler) getMetrics(w http.ResponseWriter, r *http.Request) {
	metrics, err := h.service.Metrics(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeData(w, http.StatusOK, metrics)
}

func (h *Handler) resetDemo(w http.ResponseWriter, r *http.Request) {
	if err := h.service.Reset(r.Context()); err != nil {
		writeServiceError(w, err)
		return
	}
	writeData(w, http.StatusOK, map[string]string{"message": "demo state reset"})
}

type responseEnvelope struct {
	Success bool           `json:"success"`
	Data    any            `json:"data,omitempty"`
	Error   *responseError `json:"error,omitempty"`
}

type responseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeData(w http.ResponseWriter, status int, data any) {
	writeJSON(w, status, responseEnvelope{Success: true, Data: data})
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, responseEnvelope{
		Success: false, Error: &responseError{Code: code, Message: message},
	})
}

func writeServiceError(w http.ResponseWriter, err error) {
	var validation *service.ValidationError
	switch {
	case errors.As(err, &validation):
		writeError(w, http.StatusBadRequest, "validation_error", validation.Message)
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, store.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", store.ErrIdempotencyConflict.Error())
	case errors.Is(err, store.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "invalid_transition", store.ErrInvalidTransition.Error())
	case errors.Is(err, service.ErrInjectedFault):
		writeError(w, http.StatusServiceUnavailable, "injected_fault", "retryable fault injected before Confirm")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, payload responseEnvelope) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any, allowEmpty bool) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		if allowEmpty && errors.Is(err, io.EOF) {
			return nil
		}
		var syntaxError *json.SyntaxError
		var typeError *json.UnmarshalTypeError
		switch {
		case errors.As(err, &syntaxError):
			return fmt.Errorf("malformed JSON at byte %d", syntaxError.Offset)
		case errors.As(err, &typeError):
			return fmt.Errorf("invalid value for field %s", typeError.Field)
		case strings.HasPrefix(err.Error(), "json: unknown field "):
			return err
		case errors.Is(err, io.EOF):
			return errors.New("request body must contain a JSON object")
		case err.Error() == "http: request body too large":
			return errors.New("request body must not exceed 1 MiB")
		default:
			return errors.New("invalid JSON request body")
		}
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recover() != nil {
				writeError(w, http.StatusInternalServerError, "internal_error", "unexpected internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
