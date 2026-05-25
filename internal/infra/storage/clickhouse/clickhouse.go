package clickhouse

import (
	"context"
	"fmt"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type DB struct {
	conn driver.Conn
}

type Config struct {
	Addr     string
	Database string
	Username string
	Password string
}

func New(ctx context.Context, cfg Config) (*DB, error) {
	conn, err := clickhousego.Open(&clickhousego.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhousego.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 10 * time.Minute,
		Compression: &clickhousego.Compression{
			Method: clickhousego.CompressionLZ4,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	return &DB{conn: conn}, nil
}

func (db *DB) Conn() driver.Conn { return db.conn }

func (db *DB) Ping(ctx context.Context) error { return db.conn.Ping(ctx) }

func (db *DB) Close() error { return db.conn.Close() }
