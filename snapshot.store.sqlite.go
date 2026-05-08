package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gradientzero/comby/v3"
	_ "modernc.org/sqlite"
)

// SnapshotStoreSQLiteOption configures the SQLite snapshot store.
type SnapshotStoreSQLiteOption func(*snapshotStoreSQLiteConfig)

type snapshotStoreSQLiteConfig struct {
	MaxOpenConns    int
	ConnMaxIdleTime time.Duration
}

// SnapshotStoreSQLiteWithMaxOpenConns sets the maximum number of open connections.
func SnapshotStoreSQLiteWithMaxOpenConns(n int) SnapshotStoreSQLiteOption {
	return func(c *snapshotStoreSQLiteConfig) { c.MaxOpenConns = n }
}

// SnapshotStoreSQLiteWithConnMaxIdleTime sets the maximum connection idle time.
func SnapshotStoreSQLiteWithConnMaxIdleTime(d time.Duration) SnapshotStoreSQLiteOption {
	return func(c *snapshotStoreSQLiteConfig) { c.ConnMaxIdleTime = d }
}

// Make sure it implements interfaces
var _ comby.SnapshotStore = (*snapshotStoreSQLite)(nil)

type snapshotStoreSQLite struct {
	db     *sql.DB
	config snapshotStoreSQLiteConfig
	path   string
}

func NewSnapshotStoreSQLite(path string, opts ...SnapshotStoreSQLiteOption) comby.SnapshotStore {
	s := &snapshotStoreSQLite{
		path: path,
	}
	for _, opt := range opts {
		opt(&s.config)
	}
	return s
}

func (s *snapshotStoreSQLite) connect(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("sqlite", s.path)
	if err != nil {
		return nil, err
	}

	maxOpenConns := 1
	if s.config.MaxOpenConns > 0 {
		maxOpenConns = s.config.MaxOpenConns
	}
	db.SetMaxOpenConns(maxOpenConns)

	if s.config.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(s.config.ConnMaxIdleTime)
	} else {
		db.SetConnMaxIdleTime(5 * time.Minute)
	}

	query := `
	PRAGMA journal_mode=WAL;
	PRAGMA synchronous=NORMAL;
	PRAGMA busy_timeout=5000;
	`
	if _, err := db.ExecContext(ctx, query); err != nil {
		return nil, err
	}
	return db, nil
}

func (s *snapshotStoreSQLite) migrate(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS snapshots (
		aggregate_uuid TEXT PRIMARY KEY,
		tenant_uuid TEXT,
		workspace_uuid TEXT,
		domain TEXT NOT NULL,
		version INTEGER NOT NULL,
		data BLOB NOT NULL,
		created_at INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS "snapshots_tenant_index" ON "snapshots" ("tenant_uuid" ASC);
	CREATE INDEX IF NOT EXISTS "snapshots_workspace_index" ON "snapshots" ("workspace_uuid" ASC);
	`
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return err
	}
	// migrate existing databases: add tenant_uuid + workspace_uuid columns if they don't exist
	for _, col := range []string{"tenant_uuid", "workspace_uuid"} {
		var count int
		if err := s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM pragma_table_info('snapshots') WHERE name='%s'`, col)).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE snapshots ADD COLUMN %s TEXT`, col)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *snapshotStoreSQLite) Init(ctx context.Context) error {
	db, err := s.connect(ctx)
	if err != nil {
		return err
	}
	s.db = db

	if err := s.migrate(ctx); err != nil {
		return err
	}
	return nil
}

func (s *snapshotStoreSQLite) Save(ctx context.Context, model *comby.SnapshotStoreModel) error {
	if model == nil {
		return fmt.Errorf("snapshot model is nil")
	}

	query := `INSERT INTO snapshots (aggregate_uuid, tenant_uuid, workspace_uuid, domain, version, data, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(aggregate_uuid) DO UPDATE SET
			tenant_uuid=excluded.tenant_uuid,
			workspace_uuid=excluded.workspace_uuid,
			domain=excluded.domain,
			version=excluded.version,
			data=excluded.data,
			created_at=excluded.created_at;`

	_, err := s.db.ExecContext(ctx, query,
		model.AggregateUuid,
		model.TenantUuid,
		model.WorkspaceUuid,
		model.Domain,
		model.Version,
		model.Data,
		model.CreatedAt,
	)
	return err
}

func (s *snapshotStoreSQLite) GetLatest(ctx context.Context, aggregateUuid string) (*comby.SnapshotStoreModel, error) {
	query := `SELECT aggregate_uuid, COALESCE(tenant_uuid, ''), COALESCE(workspace_uuid, ''), domain, version, data, created_at
		FROM snapshots WHERE aggregate_uuid=? LIMIT 1;`

	row := s.db.QueryRowContext(ctx, query, aggregateUuid)

	var model comby.SnapshotStoreModel
	if err := row.Scan(
		&model.AggregateUuid,
		&model.TenantUuid,
		&model.WorkspaceUuid,
		&model.Domain,
		&model.Version,
		&model.Data,
		&model.CreatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &model, nil
}

func (s *snapshotStoreSQLite) Delete(ctx context.Context, aggregateUuid string) error {
	query := `DELETE FROM snapshots WHERE aggregate_uuid=?;`
	_, err := s.db.ExecContext(ctx, query, aggregateUuid)
	return err
}

func (s *snapshotStoreSQLite) Close(ctx context.Context) error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
