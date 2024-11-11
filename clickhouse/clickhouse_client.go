package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/ClickHouse/clickhouse-go" // Import ClickHouse driver
)

type ClickHouseConfig struct {
	Host     string
	Port     string
	Database string
	Username string
	Password string
}

// NewClickHouseConfig creates a new ClickHouseConfig instance.
func NewClickHouseConfig(host, port, database, username, password string) *ClickHouseConfig {
	return &ClickHouseConfig{
		Host:     host,
		Port:     port,
		Database: database,
		Username: username,
		Password: password,
	}
}

// ClickHouseClientImpl is the actual implementation of ClickHouseClient interface.
type ClickHouseClientImpl struct {
	db *sql.DB
}

// NewClickHouseClient initializes and returns a new ClickHouse client with retries.
func NewClickHouseClient(ctx context.Context, maxAttempts int, maxDelay time.Duration, cfg *ClickHouseConfig) (*ClickHouseClientImpl, error) {
	dsn := fmt.Sprintf("tcp://%s:%s?database=%s&username=%s&password=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.Username, cfg.Password)

	var db *sql.DB
	var err error
	err = doWithAttempts(func() error {
		db, err = sql.Open("clickhouse", dsn)
		if err != nil {
			log.Println("Failed to connect to ClickHouse. Retrying...")
			return err
		}
		// Try a ping to verify the connection
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		err = db.PingContext(ctx)
		if err != nil {
			log.Println("Ping to ClickHouse failed. Retrying...")
			return err
		}
		return nil
	}, maxAttempts, maxDelay)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse after %d attempts: %w", maxAttempts, err)
	}

	return &ClickHouseClientImpl{db: db}, nil
}

// doWithAttempts provides retry logic for the client initialization.
func doWithAttempts(fn func() error, maxAttempts int, delay time.Duration) error {
	for maxAttempts > 0 {
		if err := fn(); err != nil {
			time.Sleep(delay)
			maxAttempts--
			continue
		}
		return nil
	}
	return fmt.Errorf("all attempts exhausted")
}

// Query executes a query that returns rows, typically a SELECT.
func (c *ClickHouseClientImpl) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
func (c *ClickHouseClientImpl) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}

// Exec executes a query without returning any rows, typically an INSERT, UPDATE, or DELETE.
func (c *ClickHouseClientImpl) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}
