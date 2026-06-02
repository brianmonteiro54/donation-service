package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// ---------------------------------------------------------------------------
// Mock do SQSAPI
// ---------------------------------------------------------------------------

type mockSQSClient struct {
	called   int
	lastBody string
	err      error
}

func (m *mockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	m.called++
	if params.MessageBody != nil {
		m.lastBody = *params.MessageBody
	}
	if m.err != nil {
		return nil, m.err
	}
	return &sqs.SendMessageOutput{}, nil
}

// ---------------------------------------------------------------------------
// HealthHandler
// ---------------------------------------------------------------------------

func TestHealthHandler(t *testing.T) {
	app := &App{}
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	app.HealthHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("esperava 200, obtive %d", resp.StatusCode)
	}

	body := w.Body.String()
	if !strings.Contains(body, `"status":"ok"`) {
		t.Errorf("resposta sem status ok: %s", body)
	}
	if !strings.Contains(body, `"service":"donation-service"`) {
		t.Errorf("resposta sem nome do serviço: %s", body)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("esperava Content-Type application/json, obtive %q", got)
	}
}

// ---------------------------------------------------------------------------
// DonationHandler - POST /donations
// ---------------------------------------------------------------------------

func TestDonationHandler_POST_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("falha ao criar sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id", "created_at"}).AddRow(42, time.Now())
	mock.ExpectQuery("INSERT INTO donations").
		WithArgs(1, 150.50, "João Silva", "APPROVED").
		WillReturnRows(rows)

	sqsMock := &mockSQSClient{}
	app := &App{
		DB:          db,
		SqsClient:   sqsMock,
		SqsQueueURL: "http://localstack:4566/000000000000/solidary-donations",
	}

	payload := `{"ngo_id":1,"amount":150.50,"donor_name":"João Silva"}`
	req := httptest.NewRequest(http.MethodPost, "/donations", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()

	app.DonationHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("esperava 201, obtive %d (body: %s)", w.Code, w.Body.String())
	}

	var d Donation
	if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil {
		t.Fatalf("resposta não é JSON válido: %v", err)
	}
	if d.ID != 42 {
		t.Errorf("esperava ID 42, obtive %d", d.ID)
	}
	if d.Status != "APPROVED" {
		t.Errorf("esperava status APPROVED, obtive %s", d.Status)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectativas do mock não atendidas: %v", err)
	}
}

func TestDonationHandler_POST_InvalidPayload(t *testing.T) {
	app := &App{}

	req := httptest.NewRequest(http.MethodPost, "/donations", bytes.NewBufferString("{invalid json"))
	w := httptest.NewRecorder()

	app.DonationHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("esperava 400, obtive %d", w.Code)
	}
}

func TestDonationHandler_POST_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("falha ao criar sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("INSERT INTO donations").
		WillReturnError(errors.New("conexão recusada"))

	app := &App{DB: db}

	payload := `{"ngo_id":1,"amount":10.0,"donor_name":"X"}`
	req := httptest.NewRequest(http.MethodPost, "/donations", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()

	app.DonationHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("esperava 500, obtive %d", w.Code)
	}
}

func TestDonationHandler_POST_WithoutSQS(t *testing.T) {
	// Quando SqsClient é nil, a doação deve ser salva mas nenhum evento publicado.
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id", "created_at"}).AddRow(1, time.Now())
	mock.ExpectQuery("INSERT INTO donations").WillReturnRows(rows)

	app := &App{DB: db, SqsClient: nil}

	req := httptest.NewRequest(http.MethodPost, "/donations",
		bytes.NewBufferString(`{"ngo_id":1,"amount":10,"donor_name":"X"}`))
	w := httptest.NewRecorder()

	app.DonationHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("esperava 201, obtive %d", w.Code)
	}
}

// ---------------------------------------------------------------------------
// DonationHandler - GET /donations
// ---------------------------------------------------------------------------

func TestDonationHandler_GET_ReturnsList(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id", "ngo_id", "amount", "donor_name", "status", "created_at"}).
		AddRow(1, 1, 100.00, "João", "APPROVED", time.Now()).
		AddRow(2, 2, 200.00, "Maria", "APPROVED", time.Now())
	mock.ExpectQuery("SELECT .* FROM donations").WillReturnRows(rows)

	app := &App{DB: db}

	req := httptest.NewRequest(http.MethodGet, "/donations", nil)
	w := httptest.NewRecorder()

	app.DonationHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("esperava 200, obtive %d (body: %s)", w.Code, w.Body.String())
	}

	var donations []Donation
	if err := json.Unmarshal(w.Body.Bytes(), &donations); err != nil {
		t.Fatalf("resposta não é JSON válido: %v", err)
	}
	if len(donations) != 2 {
		t.Errorf("esperava 2 doações, obtive %d", len(donations))
	}
}

// ---------------------------------------------------------------------------
// Método não permitido
// ---------------------------------------------------------------------------

func TestDonationHandler_MethodNotAllowed(t *testing.T) {
	app := &App{DB: &sql.DB{}}

	req := httptest.NewRequest(http.MethodDelete, "/donations", nil)
	w := httptest.NewRecorder()

	app.DonationHandler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("esperava 405, obtive %d", w.Code)
	}
}
