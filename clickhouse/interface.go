package clickhouse

import (
	"context"
	"database/sql"
)

type ClickHouseClient interface {
	Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row
	Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error)
	Close() error
}
