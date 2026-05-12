package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTP       HTTPConfig
	Postgres   PostgresConfig
	Kafka      KafkaConfig
	ClickHouse ClickHouseConfig
	Log        LogConfig
	OTel       OTelConfig
	App        AppConfig
}

type ClickHouseConfig struct {
	Addr     string
	Database string
	Username string
	Password string
}

type KafkaConfig struct {
	Brokers       []string
	TopicEvents   string
	TopicDLQ      string
	ConsumerGroup string
	MaxRetries    int
}

type AppConfig struct {
	IdempotencyTTL time.Duration
}

type HTTPConfig struct {
	Host         string
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	PprofEnabled bool
}

type PostgresConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type LogConfig struct {
	Level  string
	Format string
}

type OTelConfig struct {
	Enabled      bool
	ServiceName  string
	ExporterType string
	Endpoint     string
}

func Load() (*Config, error) {
	cfg := &Config{
		HTTP: HTTPConfig{
			Host:         getEnv("HTTP_HOST", "0.0.0.0"),
			Port:         getEnvInt("HTTP_PORT", 8080),
			ReadTimeout:  getEnvDuration("HTTP_READ_TIMEOUT", 15*time.Second),
			WriteTimeout: getEnvDuration("HTTP_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:  getEnvDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
			PprofEnabled: getEnvBool("PPROF_ENABLED", false),
		},
		Postgres: PostgresConfig{
			DSN:             requireEnv("POSTGRES_DSN"),
			MaxOpenConns:    getEnvInt("POSTGRES_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    getEnvInt("POSTGRES_MAX_IDLE_CONNS", 5),
			ConnMaxLifetime: getEnvDuration("POSTGRES_CONN_MAX_LIFETIME", 5*time.Minute),
		},
		Log: LogConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "json"),
		},
		ClickHouse: ClickHouseConfig{
			Addr:     getEnv("CLICKHOUSE_ADDR", "localhost:9000"),
			Database: getEnv("CLICKHOUSE_DATABASE", "events"),
			Username: getEnv("CLICKHOUSE_USER", "events"),
			Password: getEnv("CLICKHOUSE_PASSWORD", "events"),
		},
		Kafka: KafkaConfig{
			Brokers:       getEnvStringSlice("KAFKA_BROKERS", []string{"localhost:19092"}),
			TopicEvents:   getEnv("KAFKA_TOPIC_EVENTS", "events"),
			TopicDLQ:      getEnv("KAFKA_TOPIC_DLQ", "events.dlq"),
			ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "event-processor"),
			MaxRetries:    getEnvInt("KAFKA_MAX_RETRIES", 3),
		},
		OTel: OTelConfig{
			Enabled:      getEnvBool("OTEL_ENABLED", false),
			ServiceName:  getEnv("OTEL_SERVICE_NAME", "event-observability-platform"),
			ExporterType: getEnv("OTEL_EXPORTER_TYPE", "stdout"),
			Endpoint:     getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
		},
		App: AppConfig{
			IdempotencyTTL: getEnvDuration("IDEMPOTENCY_TTL", 24*time.Hour),
		},
	}
	return cfg, nil
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required environment variable %q is not set", key))
	}
	return v
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}

func getEnvBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func getEnvStringSlice(key string, fallback []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parts := strings.Split(v, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			result = append(result, s)
		}
	}
	return result
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
