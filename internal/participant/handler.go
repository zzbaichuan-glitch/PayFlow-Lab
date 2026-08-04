package participant

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/dtm-labs/dtm/client/dtmcli"
)

const barrierTable = "dtm_barrier.barrier"

type TCCRequest struct {
	PaymentID   string `json:"payment_id"`
	GID         string `json:"gid"`
	AccountID   string `json:"account_id"`
	AmountCents int64  `json:"amount_cents"`
	FailTry     bool   `json:"fail_try,omitempty"`
}

type branchBusiness func(*sql.Tx, TCCRequest) error

func branchHandler(db *sql.DB, expectedOp string, business branchBusiness) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request TCCRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid branch request: " + err.Error()})
			return
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "branch request must contain one JSON object"})
			return
		}
		barrier, err := dtmcli.BarrierFromQuery(r.URL.Query())
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if barrier.Op != expectedOp {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "DTM op does not match branch route"})
			return
		}
		if request.GID != barrier.Gid {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request gid does not match DTM barrier gid"})
			return
		}
		barrier.DBType = dtmcli.DBTypeMysql
		barrier.BarrierTableName = barrierTable
		err = barrier.CallWithDB(db, func(tx *sql.Tx) error {
			return business(tx, request)
		})
		if err == nil {
			writeJSON(w, http.StatusOK, dtmcli.MapSuccess)
			return
		}
		status, payload := dtmcli.Result2HttpJSON(err)
		writeJSON(w, status, payload)
	}
}

func healthHandler(serviceName string, db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := contextWithTimeout(r, 2*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"success": false, "error": map[string]string{"code": "mysql_unavailable", "message": err.Error()},
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data":    map[string]any{"status": "ok", "service": serviceName, "mysql": "ok", "time": time.Now().UTC()},
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func contextWithTimeout(r *http.Request, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), timeout)
}
