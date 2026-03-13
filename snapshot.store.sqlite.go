package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gradientzero/comby/v2"
	_ "modernc.org/sqlite"
)

// Make sure it implements interfaces
var _ comby.SnapshotStore = (*snapshotStoreSQLite)(nil)

type snapshotStoreSQLite struct {
	db   *sql.DB
	path string
}

func NewSnapshotStoreSQLite(path string) comby.SnapshotStore {
	return &snapshotStoreSQLite{
		path: path,
	}
}

func (s *snapshotStoreSQLite) connect(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("sqlite", s.path)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)

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
		domain TEXT NOT NULL,
		version INTEGER NOT NULL,
		data BLOB NOT NULL,
		created_at INTEGER NOT NULL
	);
	`
	_, err := s.db.ExecContext(ctx, query)
	return err
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

	query := `INSERT INTO snapshots (aggregate_uuid, domain, version, data, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(aggregate_uuid) DO UPDATE SET
			domain=excluded.domain,
			version=excluded.version,
			data=excluded.data,
			created_at=excluded.created_at;`

	_, err := s.db.ExecContext(ctx, query,
		model.AggregateUuid,
		model.Domain,
		model.Version,
		model.Data,
		model.CreatedAt,
	)
	return err
}

func (s *snapshotStoreSQLite) GetLatest(ctx context.Context, aggregateUuid string) (*comby.SnapshotStoreModel, error) {
	query := `SELECT aggregate_uuid, domain, version, data, created_at
		FROM snapshots WHERE aggregate_uuid=? LIMIT 1;`

	row := s.db.QueryRowContext(ctx, query, aggregateUuid)

	var model comby.SnapshotStoreModel
	if err := row.Scan(
		&model.AggregateUuid,
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
