package migrate

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

const (
	TargetPostgres   = "postgres"
	TargetClickHouse = "clickhouse"
	DefaultPGDir     = "migrations/postgres"
	DefaultCHDir     = "migrations/clickhouse"
)

func Run() {
	target := flag.String("target", TargetPostgres, "database target: postgres | clickhouse")
	dir := flag.String("dir", "", "migrations directory (default: auto-detected by target)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		PrintUsage()
		os.Exit(1)
	}

	migrationsDir := *dir
	if migrationsDir == "" {
		switch *target {
		case TargetPostgres:
			migrationsDir = DefaultPGDir
		case TargetClickHouse:
			migrationsDir = DefaultCHDir
		default:
			log.Fatalf("unknown target %q: must be %q or %q", *target, TargetPostgres, TargetClickHouse)
		}
	}

	db, driver := OpenDB(*target)
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("db close: %v", err)
		}
	}()

	if err := goose.SetDialect(driver); err != nil {
		log.Fatalf("set dialect: %v", err)
	}

	command := args[0]
	commandArgs := args[1:]

	if err := goose.RunContext(context.Background(), command, db, migrationsDir, commandArgs...); err != nil {
		log.Fatalf("migrate %s: %v", command, err)
	}
}

func OpenDB(target string) (*sql.DB, string) {
	switch target {
	case TargetPostgres:
		dsn := RequiredEnv("POSTGRES_DSN")
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			log.Fatalf("open postgres: %v", err)
		}
		return db, "postgres"

	case TargetClickHouse:
		dsn := RequiredEnv("CLICKHOUSE_DSN")
		db, err := sql.Open("clickhouse", dsn)
		if err != nil {
			log.Fatalf("open clickhouse: %v", err)
		}
		return db, "clickhouse"

	default:
		log.Fatalf("unknown target %q", target)
		panic("unreachable")
	}
}

func RequiredEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s is required", key)
	}
	return v
}

func PrintUsage() {
	fmt.Fprintf(os.Stderr, `Usage: migrate [flags] <command> [args]

Flags:
  -target string   database target: postgres | clickhouse (default: postgres)
  -dir   string    migrations directory (default: auto by target)

Commands:
  up               Apply all pending migrations
  up-to VERSION    Migrate up to a specific version
  down             Rollback the last applied migration
  down-to VERSION  Rollback to a specific version
  status           Print migration status
  create NAME      Create a new migration file (sql)
  reset            Rollback all migrations
  version          Print current migration version

Environment variables:
  POSTGRES_DSN     PostgreSQL DSN (e.g. postgres://user:pass@host/db?sslmode=disable)
  CLICKHOUSE_DSN   ClickHouse DSN (e.g. clickhouse://user:pass@host:9000/db)

Examples:
  migrate up
  migrate -target=clickhouse up
  migrate status
  migrate -target=postgres create add_user_sessions
  migrate down-to 1
`)
}
