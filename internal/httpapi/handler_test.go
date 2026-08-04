package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"payflow-lab/internal/httpapi"
	"payflow-lab/internal/service"
	"payflow-lab/internal/store"
)

type testEnvelope struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
	Error   *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func TestSystemInfoDoesNotAdvertiseDTMInMemoryMode(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(httpapi.NewHandler(service.NewPaymentService(store.NewMemoryStore())))
	defer server.Close()

	envelope := getEnvelope(t, server.URL+"/api/v1/system", http.StatusOK)
	var info struct {
		Mode           string          `json:"mode"`
		DistributedTCC bool            `json:"distributed_tcc"`
		Coordinator    json.RawMessage `json:"coordinator"`
		FaultPoints    []string        `json:"fault_points"`
	}
	decodeData(t, envelope, &info)
	if info.Mode != "memory" || info.DistributedTCC {
		t.Fatalf("unexpected memory system info: %#v", info)
	}
	if len(info.Coordinator) != 0 || len(info.FaultPoints) != 0 {
		t.Fatalf("memory mode must not advertise DTM or distributed faults: %#v", info)
	}
}

func TestHTTPPaymentLifecycle(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(httpapi.NewHandler(service.NewPaymentService(store.NewMemoryStore())))
	defer server.Close()

	health := getEnvelope(t, server.URL+"/healthz", http.StatusOK)
	if !health.Success {
		t.Fatalf("health response was not successful: %#v", health)
	}

	requestBody := []byte(`{"order_id":"order-http-1","account_id":"demo-user","amount_cents":2500}`)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/payments", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "idem-http-1")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	created := decodeEnvelope(t, response, http.StatusCreated)
	var createData struct {
		Payment struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"payment"`
		IdempotentReplay bool `json:"idempotent_replay"`
	}
	decodeData(t, created, &createData)
	if createData.Payment.ID == "" || createData.Payment.Status != "PROCESSING" || createData.IdempotentReplay {
		t.Fatalf("unexpected create data: %#v", createData)
	}

	details := getEnvelope(t, server.URL+"/api/v1/payments/"+createData.Payment.ID, http.StatusOK)
	var detailData struct {
		Events []json.RawMessage `json:"events"`
	}
	decodeData(t, details, &detailData)
	if len(detailData.Events) < 3 {
		t.Fatalf("event count = %d, want at least 3", len(detailData.Events))
	}

	callbackBody := []byte(`{"event_id":"http-callback-1","sequence":1,"outcome":"success"}`)
	callbackResponse := postJSON(t, server.URL+"/api/v1/payments/"+createData.Payment.ID+"/callbacks", callbackBody, "")
	callback := decodeEnvelope(t, callbackResponse, http.StatusOK)
	var callbackData struct {
		Disposition string `json:"disposition"`
		Payment     struct {
			Status string `json:"status"`
		} `json:"payment"`
	}
	decodeData(t, callback, &callbackData)
	if callbackData.Disposition != "applied" || callbackData.Payment.Status != "SUCCESS" {
		t.Fatalf("unexpected callback: %#v", callbackData)
	}

	metrics := getEnvelope(t, server.URL+"/api/v1/metrics", http.StatusOK)
	var metricData struct {
		PaymentsTotal         int    `json:"payments_total"`
		LedgerEntries         int    `json:"ledger_entries"`
		CallbacksAppliedTotal uint64 `json:"callbacks_applied_total"`
	}
	decodeData(t, metrics, &metricData)
	if metricData.PaymentsTotal != 1 || metricData.LedgerEntries != 1 || metricData.CallbacksAppliedTotal != 1 {
		t.Fatalf("unexpected metrics: %#v", metricData)
	}
}

func TestHTTPJSONErrorsUseEnvelope(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(httpapi.NewHandler(service.NewPaymentService(store.NewMemoryStore())))
	defer server.Close()

	response := postJSON(t, server.URL+"/api/v1/payments", []byte(`{"unknown":true}`), "idem-invalid")
	payload := decodeEnvelope(t, response, http.StatusBadRequest)
	if payload.Success || payload.Error == nil || payload.Error.Code != "invalid_json" {
		t.Fatalf("unexpected invalid JSON envelope: %#v", payload)
	}

	notFound := getEnvelope(t, server.URL+"/api/v1/does-not-exist", http.StatusNotFound)
	if notFound.Success || notFound.Error == nil || notFound.Error.Code != "not_found" {
		t.Fatalf("unexpected not-found envelope: %#v", notFound)
	}
}

func getEnvelope(t *testing.T, url string, expectedStatus int) testEnvelope {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	return decodeEnvelope(t, response, expectedStatus)
}

func postJSON(t *testing.T, url string, body []byte, idempotencyKey string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeEnvelope(t *testing.T, response *http.Response, expectedStatus int) testEnvelope {
	t.Helper()
	defer response.Body.Close()
	if response.StatusCode != expectedStatus {
		t.Fatalf("status = %d, want %d", response.StatusCode, expectedStatus)
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "application/json; charset=utf-8" {
		t.Fatalf("content-type = %q", contentType)
	}
	var result testEnvelope
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return result
}

func decodeData(t *testing.T, envelope testEnvelope, destination any) {
	t.Helper()
	if !envelope.Success {
		t.Fatalf("response error: %#v", envelope.Error)
	}
	if err := json.Unmarshal(envelope.Data, destination); err != nil {
		t.Fatalf("decode data: %v", err)
	}
}
