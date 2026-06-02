package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// ---------------------------------------------------------------------------
// failingResponseWriter: ResponseWriter cujo Write sempre falha. Permite
// exercitar os ramos de tratamento de erro de escrita/serialização.
// ---------------------------------------------------------------------------

type failingResponseWriter struct {
	header            http.Header
	status            int
	writeCalled       bool
	writeHeaderCalled bool
}

func (f *failingResponseWriter) Header() http.Header {
	if f.header == nil {
		f.header = http.Header{}
	}
	return f.header
}

func (f *failingResponseWriter) Write(b []byte) (int, error) {
	f.writeCalled = true
	return 0, errors.New("falha de escrita simulada")
}

func (f *failingResponseWriter) WriteHeader(statusCode int) {
	f.writeHeaderCalled = true
	f.status = statusCode
}

// ---------------------------------------------------------------------------
// getPort
// ---------------------------------------------------------------------------

func TestGetPort(t *testing.T) {
	t.Setenv("PORT", "9999")
	if got := getPort(); got != "9999" {
		t.Errorf("esperava 9999, obtive %q", got)
	}

	if err := os.Unsetenv("PORT"); err != nil {
		t.Fatalf("falha ao limpar PORT: %v", err)
	}
	if got := getPort(); got != "8082" {
		t.Errorf("esperava default 8082, obtive %q", got)
	}
}

// ---------------------------------------------------------------------------
// newSQSClient
// ---------------------------------------------------------------------------

func TestNewSQSClient_NotConfigured(t *testing.T) {
	ctx := context.Background()

	c, err := newSQSClient(ctx, "", "us-east-1", "")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if c != nil {
		t.Errorf("esperava nil quando queueURL vazio, obtive %v", c)
	}

	c, err = newSQSClient(ctx, "http://fila", "", "")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if c != nil {
		t.Errorf("esperava nil quando region vazio, obtive %v", c)
	}
}

func TestNewSQSClient_Configured(t *testing.T) {
	ctx := context.Background()

	// Com endpoint customizado (LocalStack)
	c, err := newSQSClient(ctx, "http://localstack:4566/000000000000/q", "us-east-1", "http://localstack:4566")
	if err != nil {
		t.Fatalf("erro inesperado (endpoint custom): %v", err)
	}
	if c == nil {
		t.Fatal("esperava cliente SQS, obtive nil (endpoint custom)")
	}

	// Sem endpoint (produção)
	c, err = newSQSClient(ctx, "http://fila", "us-east-1", "")
	if err != nil {
		t.Fatalf("erro inesperado (produção): %v", err)
	}
	if c == nil {
		t.Fatal("esperava cliente SQS, obtive nil (produção)")
	}
}

// ---------------------------------------------------------------------------
// buildApp
// ---------------------------------------------------------------------------

func TestBuildApp_NoDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	if _, err := buildApp(context.Background()); err == nil {
		t.Fatal("esperava erro quando DATABASE_URL está ausente")
	}
}

func TestBuildApp_Success(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/donations?sslmode=disable")
	t.Setenv("AWS_SQS_URL", "")
	t.Setenv("AWS_REGION", "")

	app, err := buildApp(context.Background())
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if app == nil || app.DB == nil {
		t.Fatal("esperava App com DB inicializado")
	}
	if app.SqsClient != nil {
		t.Error("esperava SqsClient nil quando AWS não configurado")
	}
	_ = app.DB.Close()
}

func TestBuildApp_WithSQS(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/donations?sslmode=disable")
	t.Setenv("AWS_SQS_URL", "http://localstack:4566/000000000000/q")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ENDPOINT_URL", "http://localstack:4566")

	app, err := buildApp(context.Background())
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if app.SqsClient == nil {
		t.Error("esperava SqsClient configurado quando AWS_SQS_URL e AWS_REGION definidos")
	}
	_ = app.DB.Close()
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

func TestRouter(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("falha ao criar sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id", "ngo_id", "amount", "donor_name", "status", "created_at"}).
		AddRow(1, 1, 10.0, "X", "APPROVED", time.Now())
	mock.ExpectQuery("SELECT .* FROM donations").WillReturnRows(rows)

	app := &App{DB: db}
	srv := httptest.NewServer(app.Router())
	defer srv.Close()

	check := func(path string, wantStatus int) {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		if wantStatus != 0 && resp.StatusCode != wantStatus {
			t.Errorf("GET %s: esperava %d, obtive %d", path, wantStatus, resp.StatusCode)
		}
	}

	check("/health", http.StatusOK)
	check("/donations", http.StatusOK)
	check("/metrics", 0)      // basta não dar erro de transporte
	check("/desconhecida", 0) // rota não registrada
}

// ---------------------------------------------------------------------------
// Serve
// ---------------------------------------------------------------------------

func TestServe_InvalidPort(t *testing.T) {
	app := &App{}
	// Porta fora do intervalo válido faz o ListenAndServe retornar erro de imediato.
	if err := app.Serve("99999"); err == nil {
		t.Fatal("esperava erro ao escutar em porta inválida, obtive nil")
	}
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

func TestRun_BuildAppError(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	if err := run(); err == nil {
		t.Fatal("esperava erro quando DATABASE_URL está ausente")
	}
}

func TestRun_PingError(t *testing.T) {
	// DSN aponta para uma porta sem servidor: o Ping falha rapidamente
	// (conexão recusada / timeout curto), antes de chamar Serve.
	t.Setenv("DATABASE_URL", "postgres://user:pass@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	t.Setenv("AWS_SQS_URL", "")
	t.Setenv("AWS_REGION", "")

	if err := run(); err == nil {
		t.Fatal("esperava erro de Ping com banco inacessível")
	}
}

// ---------------------------------------------------------------------------
// initOTel
// ---------------------------------------------------------------------------

func TestInitOTel(t *testing.T) {
	cleanup, err := initOTel(context.Background())
	if err != nil {
		t.Fatalf("erro inesperado ao inicializar OTel: %v", err)
	}
	if cleanup == nil {
		t.Fatal("esperava função de cleanup não-nil")
	}
	cleanup()
}

// ---------------------------------------------------------------------------
// sendNotificationEvent
// ---------------------------------------------------------------------------

func TestSendNotificationEvent_Success(t *testing.T) {
	mockC := &mockSQSClient{}
	app := &App{SqsClient: mockC, SqsQueueURL: "http://fila"}

	app.sendNotificationEvent(Donation{ID: 1, DonorName: "X", Amount: 10})

	if mockC.called != 1 {
		t.Errorf("esperava 1 chamada a SendMessage, obtive %d", mockC.called)
	}
	if mockC.lastBody == "" {
		t.Error("esperava corpo da mensagem preenchido")
	}
}

func TestSendNotificationEvent_Error(t *testing.T) {
	mockC := &mockSQSClient{err: errors.New("falha no SQS")}
	app := &App{SqsClient: mockC, SqsQueueURL: "http://fila"}

	// Não deve entrar em pânico mesmo quando SendMessage falha.
	app.sendNotificationEvent(Donation{ID: 2})

	if mockC.called != 1 {
		t.Errorf("esperava 1 chamada a SendMessage, obtive %d", mockC.called)
	}
}

// ---------------------------------------------------------------------------
// DonationHandler - ramos adicionais (GET)
// ---------------------------------------------------------------------------

func TestDonationHandler_GET_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT .* FROM donations").WillReturnError(errors.New("erro de query"))

	app := &App{DB: db}
	req := httptest.NewRequest(http.MethodGet, "/donations", nil)
	w := httptest.NewRecorder()

	app.DonationHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("esperava 500, obtive %d", w.Code)
	}
}

func TestDonationHandler_GET_ScanError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	// "abc" não converte para o campo int ID -> erro de Scan.
	rows := sqlmock.NewRows([]string{"id", "ngo_id", "amount", "donor_name", "status", "created_at"}).
		AddRow("abc", 1, 10.0, "X", "APPROVED", time.Now())
	mock.ExpectQuery("SELECT .* FROM donations").WillReturnRows(rows)

	app := &App{DB: db}
	req := httptest.NewRequest(http.MethodGet, "/donations", nil)
	w := httptest.NewRecorder()

	app.DonationHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("esperava 500 por erro de scan, obtive %d", w.Code)
	}
}

func TestDonationHandler_GET_RowsErr(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id", "ngo_id", "amount", "donor_name", "status", "created_at"}).
		AddRow(1, 1, 10.0, "X", "APPROVED", time.Now()).
		RowError(0, errors.New("erro ao iterar"))
	mock.ExpectQuery("SELECT .* FROM donations").WillReturnRows(rows)

	app := &App{DB: db}
	req := httptest.NewRequest(http.MethodGet, "/donations", nil)
	w := httptest.NewRecorder()

	app.DonationHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("esperava 500 por rows.Err, obtive %d", w.Code)
	}
}

func TestDonationHandler_GET_CloseError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	// Uma linha inválida ("abc" no ID) provoca erro de Scan e retorno antecipado,
	// antes de a iteração esgotar. Com isso o defer é o primeiro a fechar o cursor
	// e o CloseError aflora no rows.Close() explícito, exercitando o ramo de log
	// do defer ("Erro ao fechar o cursor de doações").
	rows := sqlmock.NewRows([]string{"id", "ngo_id", "amount", "donor_name", "status", "created_at"}).
		AddRow("abc", 1, 10.0, "X", "APPROVED", time.Now()).
		CloseError(errors.New("erro ao fechar cursor"))
	mock.ExpectQuery("SELECT .* FROM donations").WillReturnRows(rows)

	app := &App{DB: db}
	req := httptest.NewRequest(http.MethodGet, "/donations", nil)
	w := httptest.NewRecorder()

	app.DonationHandler(w, req)

	// O erro de Scan produz 500; o CloseError é apenas logado no defer.
	if w.Code != http.StatusInternalServerError {
		t.Errorf("esperava 500, obtive %d", w.Code)
	}
}

func TestDonationHandler_GET_EncodeError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id", "ngo_id", "amount", "donor_name", "status", "created_at"}).
		AddRow(1, 1, 10.0, "X", "APPROVED", time.Now())
	mock.ExpectQuery("SELECT .* FROM donations").WillReturnRows(rows)

	app := &App{DB: db}
	req := httptest.NewRequest(http.MethodGet, "/donations", nil)
	fw := &failingResponseWriter{header: http.Header{}}

	app.DonationHandler(fw, req)

	if !fw.writeCalled {
		t.Error("esperava tentativa de escrita ao serializar a lista")
	}
}

// ---------------------------------------------------------------------------
// DonationHandler - ramo adicional (POST)
// ---------------------------------------------------------------------------

func TestDonationHandler_POST_EncodeError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id", "created_at"}).AddRow(7, time.Now())
	mock.ExpectQuery("INSERT INTO donations").WillReturnRows(rows)

	app := &App{DB: db}
	body := bytes.NewBufferString(`{"ngo_id":1,"amount":5,"donor_name":"Y"}`)
	req := httptest.NewRequest(http.MethodPost, "/donations", body)
	fw := &failingResponseWriter{header: http.Header{}}

	app.DonationHandler(fw, req)

	if !fw.writeHeaderCalled || fw.status != http.StatusCreated {
		t.Errorf("esperava WriteHeader(201), status obtido %d", fw.status)
	}
	if !fw.writeCalled {
		t.Error("esperava tentativa de escrita ao serializar a resposta")
	}
}

// ---------------------------------------------------------------------------
// HealthHandler - ramo de erro de escrita
// ---------------------------------------------------------------------------

func TestHealthHandler_WriteError(t *testing.T) {
	app := &App{}
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	fw := &failingResponseWriter{header: http.Header{}}

	app.HealthHandler(fw, req)

	if !fw.writeHeaderCalled || fw.status != http.StatusOK {
		t.Errorf("esperava WriteHeader(200), status obtido %d", fw.status)
	}
	if !fw.writeCalled {
		t.Error("esperava tentativa de escrita da resposta de health")
	}
}
