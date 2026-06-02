package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

type Donation struct {
	ID        int       `json:"id"`
	NgoID     int       `json:"ngo_id"`
	Amount    float64   `json:"amount"`
	DonorName string    `json:"donor_name"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// SQSAPI define o subconjunto mínimo da API do SQS que usamos.
// Permite injetar mocks nos testes (o *sqs.Client real satisfaz esta interface).
type SQSAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type App struct {
	DB          *sql.DB
	SqsClient   SQSAPI
	SqsQueueURL string
}

// initOTel configura OpenTelemetry (traces + métricas via OTLP/gRPC).
// Espelha o padrão do auth-service. O endpoint vem de OTEL_EXPORTER_OTLP_ENDPOINT
// (default: coletor no namespace monitoring).
func initOTel(ctx context.Context) (func(), error) {
	otelEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelEndpoint == "" {
		otelEndpoint = "otel-collector-opentelemetry-collector.monitoring.svc.cluster.local:4317"
	}

	res, _ := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName("donation-service"),
		semconv.ServiceNamespace("solidarytech"),
	))

	traceExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otelEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(otelEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(15*time.Second),
		)),
	)
	otel.SetMeterProvider(mp)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(c)
		_ = mp.Shutdown(c)
	}, nil
}

// getPort retorna a porta de escuta a partir da env PORT (default 8082).
func getPort() string {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	return port
}

// newSQSClient cria um cliente SQS quando queueURL e region estão definidos.
// Retorna (nil, nil) quando a integração não está configurada (comportamento
// idêntico ao original, em que o cliente permanecia nil).
func newSQSClient(ctx context.Context, queueURL, region, endpoint string) (SQSAPI, error) {
	if queueURL == "" || region == "" {
		return nil, nil
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar config AWS: %w", err)
	}

	// Opções do cliente SQS - aponta para LocalStack quando endpoint customizado for definido
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			log.Printf("Integração com SQS ativada via endpoint customizado: %s", endpoint)
		} else {
			log.Println("Integração com AWS SQS (produção) ativada.")
		}
	})
	return client, nil
}

// buildApp monta a App a partir da configuração do ambiente. Abre o pool do
// banco (lazy, sem estabelecer conexão) e configura o cliente SQS opcional.
func buildApp(ctx context.Context) (*App, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("DATABASE_URL é obrigatória")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir conexão com o banco: %w", err)
	}

	// ---- AWS SDK v2 ----
	queueURL := os.Getenv("AWS_SQS_URL")
	region := os.Getenv("AWS_REGION")
	endpoint := os.Getenv("AWS_ENDPOINT_URL") // Para LocalStack em ambiente local

	sqsClient, err := newSQSClient(ctx, queueURL, region, endpoint)
	if err != nil {
		return nil, err
	}

	return &App{DB: db, SqsClient: sqsClient, SqsQueueURL: queueURL}, nil
}

// Router constrói o handler HTTP com as rotas, /metrics e a instrumentação OTel.
func (a *App) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.HealthHandler)
	mux.HandleFunc("/donations", a.DonationHandler)

	// Expõe /metrics para o Prometheus (ServiceMonitor faz scrape aqui)
	mux.Handle("/metrics", promhttp.Handler())

	// Wrap com OTel para gerar spans das requisições HTTP
	return otelhttp.NewHandler(mux, "donation-service")
}

// Serve inicia o servidor HTTP na porta informada (chamada bloqueante).
func (a *App) Serve(port string) error {
	log.Printf("donation-service rodando na porta %s", port)
	return http.ListenAndServe(":"+port, a.Router())
}

// run concentra a lógica de inicialização para permitir testes; main apenas a invoca.
func run() error {
	_ = godotenv.Load()
	ctx := context.Background()

	cleanup, err := initOTel(ctx)
	if err != nil {
		log.Printf("Aviso: OTel não inicializado: %v", err)
	} else {
		defer cleanup()
	}

	app, err := buildApp(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := app.DB.Close(); cerr != nil {
			log.Printf("Erro ao fechar conexão com o banco: %v", cerr)
		}
	}()

	if err := app.DB.Ping(); err != nil {
		return fmt.Errorf("erro ao pingar o banco: %w", err)
	}
	log.Println("Conectado ao PostgreSQL (donation-service).")

	return app.Serve(getPort())
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func (a *App) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(`{"status":"ok","service":"donation-service"}`)); err != nil {
		log.Printf("Erro ao escrever resposta de health: %v", err)
	}
}

func (a *App) DonationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodPost {
		var d Donation
		if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
			http.Error(w, `{"error":"Payload inválido"}`, http.StatusBadRequest)
			return
		}

		d.Status = "APPROVED" // Simulação de gateway de pagamento
		err := a.DB.QueryRow(
			"INSERT INTO donations (ngo_id, amount, donor_name, status) VALUES ($1, $2, $3, $4) RETURNING id, created_at",
			d.NgoID, d.Amount, d.DonorName, d.Status,
		).Scan(&d.ID, &d.CreatedAt)

		if err != nil {
			log.Printf("Erro ao salvar doação: %v", err)
			http.Error(w, `{"error":"Erro interno"}`, http.StatusInternalServerError)
			return
		}

		if a.SqsClient != nil {
			go a.sendNotificationEvent(d)
		}

		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(d); err != nil {
			log.Printf("Erro ao serializar resposta da doação: %v", err)
		}
		return
	}

	if r.Method == http.MethodGet {
		rows, err := a.DB.Query("SELECT id, ngo_id, amount, donor_name, status, created_at FROM donations ORDER BY id DESC")
		if err != nil {
			http.Error(w, `{"error":"Erro interno"}`, http.StatusInternalServerError)
			return
		}
		defer func() {
			if cerr := rows.Close(); cerr != nil {
				log.Printf("Erro ao fechar o cursor de doações: %v", cerr)
			}
		}()

		donations := []Donation{}
		for rows.Next() {
			var d Donation
			if err := rows.Scan(&d.ID, &d.NgoID, &d.Amount, &d.DonorName, &d.Status, &d.CreatedAt); err != nil {
				log.Printf("Erro ao ler linha de doação: %v", err)
				http.Error(w, `{"error":"Erro interno"}`, http.StatusInternalServerError)
				return
			}
			donations = append(donations, d)
		}
		if err := rows.Err(); err != nil {
			log.Printf("Erro ao iterar doações: %v", err)
			http.Error(w, `{"error":"Erro interno"}`, http.StatusInternalServerError)
			return
		}

		if err := json.NewEncoder(w).Encode(donations); err != nil {
			log.Printf("Erro ao serializar lista de doações: %v", err)
		}
		return
	}

	http.Error(w, `{"error":"Método não permitido"}`, http.StatusMethodNotAllowed)
}

func (a *App) sendNotificationEvent(d Donation) {
	body, err := json.Marshal(d)
	if err != nil {
		log.Printf("Falha ao serializar doação para evento SQS: %v", err)
		return
	}

	// Context com timeout de 10 segundos para evitar goroutines penduradas
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = a.SqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(string(body)),
		QueueUrl:    aws.String(a.SqsQueueURL),
	})
	if err != nil {
		log.Printf("Falha ao despachar evento SQS: %v", err)
	}
}