package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gradientzero/comby-store-sqlite/internal"
	"github.com/gradientzero/comby/v2"
	_ "modernc.org/sqlite"
)

// Make sure it implements interfaces
var _ comby.CommandStore = (*commandStoreSQLite)(nil)

type commandStoreSQLite struct {
	options comby.CommandStoreOptions
	db      *sql.DB

	// sqlite specific options
	path string
}

func NewCommandStoreSQLite(path string, opts ...comby.CommandStoreOption) comby.CommandStore {
	cs := &commandStoreSQLite{
		path: path,
	}
	for _, opt := range opts {
		if _, err := opt(&cs.options); err != nil {
			return nil
		}
	}
	return cs
}

func (cs *commandStoreSQLite) connect(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("sqlite", cs.path)
	if err != nil {
		return nil, err
	}
	// WAL mode allows concurrent readers while a single writer holds the lock.
	db.SetMaxOpenConns(100)

	// set sqlite specific pragmas
	query := `
		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=NORMAL;
		PRAGMA foreign_keys=1;
		PRAGMA busy_timeout=5000;
		`
	if _, err := db.ExecContext(context.Background(), query); err != nil {
		return nil, err
	}

	return db, nil
}

func (cs *commandStoreSQLite) migrate(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS commands (id INTEGER,
		instance_id INTEGER,
		uuid TEXT,
		tenant_uuid TEXT,
		domain TEXT,
		created_at INTEGER,
		data_type TEXT,
		data_bytes TEXT,
		req_ctx TEXT,
		PRIMARY KEY (id)
	);
	CREATE INDEX IF NOT EXISTS "tenant_index" ON "commands" (
		"tenant_uuid" ASC
	);
	CREATE UNIQUE INDEX IF NOT EXISTS "uuid_index" ON "commands" (
		"uuid" ASC
	);
	CREATE INDEX IF NOT EXISTS "created_at_index" ON "commands" (
		"created_at" ASC
	);
	`
	_, err := cs.db.ExecContext(ctx, query)
	return err
}

// fullfilling CommandStore interface
func (cs *commandStoreSQLite) Init(ctx context.Context, opts ...comby.CommandStoreOption) error {
	for _, opt := range opts {
		if _, err := opt(&cs.options); err != nil {
			return err
		}
	}

	// connect to db (or create new one)
	if db, err := cs.connect(ctx); err != nil {
		return err
	} else {
		cs.db = db
	}

	// auto-migrate table
	if !cs.options.ReadOnly {
		if err := cs.migrate(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (cs *commandStoreSQLite) Create(ctx context.Context, opts ...comby.CommandStoreCreateOption) error {
	createOpts := comby.CommandStoreCreateOptions{
		Command: nil,
	}
	for _, opt := range opts {
		if _, err := opt(&createOpts); err != nil {
			return err
		}
	}
	if cs.options.ReadOnly {
		return fmt.Errorf("'%s' failed to create command - instance is readonly", cs.String())
	}
	var cmd comby.Command = createOpts.Command
	if cmd == nil {
		return fmt.Errorf("'%s' failed to create command - command is nil", cs.String())
	}
	if len(cmd.GetCommandUuid()) < 1 {
		return fmt.Errorf("'%s' failed to create command - command uuid is invalid", cs.String())
	}

	// sql statement
	dbRecord, err := internal.BaseCommandToDbCommand(cmd)
	if err != nil {
		return err
	}

	// encrypt domain data if crypto service is provided
	if cs.options.CryptoService != nil {
		if err := cs.encryptDomainData(dbRecord); err != nil {
			return err
		}
	}

	// sql begin transaction
	tx, err := cs.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	query := `INSERT INTO commands (
		instance_id,
		uuid,
		tenant_uuid,
		domain,
		created_at,
		data_type,
		data_bytes,
		req_ctx
	) VALUES (?,?,?,?,?,?,?,?);`

	_, err = tx.ExecContext(
		ctx,
		query,
		dbRecord.InstanceId,
		dbRecord.Uuid,
		dbRecord.TenantUuid,
		dbRecord.Domain,
		dbRecord.CreatedAt,
		dbRecord.DataType,
		dbRecord.DataBytes,
		dbRecord.ReqCtx,
	)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (cs *commandStoreSQLite) Get(ctx context.Context, opts ...comby.CommandStoreGetOption) (comby.Command, error) {
	getOpts := comby.CommandStoreGetOptions{}
	for _, opt := range opts {
		if _, err := opt(&getOpts); err != nil {
			return nil, err
		}
	}

	if len(getOpts.CommandUuid) == 0 {
		return nil, fmt.Errorf("'%s' failed to get command - command uuid is required", cs.String())
	}

	query := `SELECT id, instance_id, uuid, tenant_uuid, domain, created_at,
		data_type, data_bytes, req_ctx
		FROM commands WHERE uuid=? LIMIT 1;`
	row := cs.db.QueryRowContext(ctx, query, getOpts.CommandUuid)
	if row.Err() != nil {
		return nil, row.Err()
	}

	// extract record
	var dbRecord internal.Command
	if err := row.Scan(
		&dbRecord.ID,
		&dbRecord.InstanceId,
		&dbRecord.Uuid,
		&dbRecord.TenantUuid,
		&dbRecord.Domain,
		&dbRecord.CreatedAt,
		&dbRecord.DataType,
		&dbRecord.DataBytes,
		&dbRecord.ReqCtx,
	); err != nil {
		// Catch errors
		switch {
		case err == sql.ErrNoRows:
			return nil, nil
		case err != nil:
			return nil, err
		}
	}

	// decrypt domain data if crypto service is provided
	if cs.options.CryptoService != nil {
		if err := cs.decryptDomainData(&dbRecord); err != nil {
			return nil, err
		}
	}

	// db record to command
	cmd, err := internal.DbCommandToBaseCommand(&dbRecord)
	if err != nil {
		return nil, err
	}
	return cmd, err
}

func (cs *commandStoreSQLite) List(ctx context.Context, opts ...comby.CommandStoreListOption) ([]comby.Command, int64, error) {
	listOpts := comby.CommandStoreListOptions{
		Before:    -1,
		After:     -1,
		Offset:    0,
		Limit:     100,
		OrderBy:   "created_at",
		Ascending: true,
	}
	for _, opt := range opts {
		if _, err := opt(&listOpts); err != nil {
			return nil, 0, err
		}
	}

	var whereSQL string = ""
	var whereList []string = []string{}
	var args []any
	if len(listOpts.TenantUuid) > 0 {
		whereList = append(whereList, "tenant_uuid=?")
		args = append(args, listOpts.TenantUuid)
	}
	if len(listOpts.Domain) > 0 {
		whereList = append(whereList, "domain=?")
		args = append(args, listOpts.Domain)
	}
	if len(listOpts.DataType) > 0 {
		whereList = append(whereList, "data_type=?")
		args = append(args, listOpts.DataType)
	}
	if listOpts.Before >= 0 {
		whereList = append(whereList, "created_at<?")
		args = append(args, listOpts.Before)
	}
	if listOpts.After >= 0 {
		whereList = append(whereList, "created_at>?")
		args = append(args, listOpts.After)
	}

	// note the first empty character(s) below
	for index, where := range whereList {
		if index == 0 {
			whereSQL = fmt.Sprintf(" WHERE %s", where)
		} else {
			whereSQL = fmt.Sprintf("%s AND %s", whereSQL, where)
		}
	}

	// count the total number of records for this query
	var queryTotal int64
	var queryTotalQuery string = fmt.Sprintf("SELECT COUNT(id) FROM commands%s;", whereSQL)
	var row *sql.Row
	if len(args) > 0 {
		row = cs.db.QueryRowContext(ctx, queryTotalQuery, args...)
	} else {
		row = cs.db.QueryRowContext(ctx, queryTotalQuery)
	}
	if err := row.Err(); err != nil {
		return nil, 0, err
	}
	// extract record
	if err := row.Scan(&queryTotal); err != nil {
		return nil, 0, err
	}

	// prepare orderby
	var orderBySQL string = ""
	if len(listOpts.OrderBy) > 0 {
		if listOpts.Ascending {
			orderBySQL = fmt.Sprintf(" ORDER BY %s ASC", listOpts.OrderBy)
		} else {
			orderBySQL = fmt.Sprintf(" ORDER BY %s DESC", listOpts.OrderBy)
		}
	}

	// prepare limit/offset
	var limitSQL string = ""
	var offsetSQL string = ""
	if listOpts.Limit >= 0 {
		limitSQL = fmt.Sprintf(" LIMIT %d", listOpts.Limit)
	}
	if listOpts.Offset >= 0 {
		offsetSQL = fmt.Sprintf(" OFFSET %d", listOpts.Offset)
	}

	var query string = fmt.Sprintf("SELECT id, instance_id, uuid, tenant_uuid, domain, created_at, data_type, data_bytes, req_ctx FROM commands%s%s%s%s;", whereSQL, orderBySQL, limitSQL, offsetSQL)
	var rows *sql.Rows
	var err error
	if len(args) > 0 {
		rows, err = cs.db.QueryContext(ctx, query, args...)
	} else {
		rows, err = cs.db.QueryContext(ctx, query)
	}
	switch {
	case err == sql.ErrNoRows:
		return nil, queryTotal, nil
	case err != nil:
		return nil, 0, err
	}
	if rows != nil {
		defer rows.Close()
	}

	// extract results
	var dbRecords []*internal.Command
	for rows.Next() {
		var dbRecord internal.Command
		if err := rows.Scan(
			&dbRecord.ID,
			&dbRecord.InstanceId,
			&dbRecord.Uuid,
			&dbRecord.TenantUuid,
			&dbRecord.Domain,
			&dbRecord.CreatedAt,
			&dbRecord.DataType,
			&dbRecord.DataBytes,
			&dbRecord.ReqCtx,
		); err != nil {
			return nil, 0, err
		}
		dbRecords = append(dbRecords, &dbRecord)
	}
	if err := rows.Close(); err != nil {
		return nil, 0, err
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	// decrypt domain data if crypto service is provided
	if cs.options.CryptoService != nil {
		for _, dbRecord := range dbRecords {
			if err := cs.decryptDomainData(dbRecord); err != nil {
				return nil, 0, err
			}
		}
	}

	// convert
	cmds, err := internal.DbCommandsToBaseCommands(dbRecords)
	if err != nil {
		return nil, 0, err
	}
	return cmds, queryTotal, err
}

func (cs *commandStoreSQLite) Update(ctx context.Context, opts ...comby.CommandStoreUpdateOption) error {
	updateOpts := comby.CommandStoreUpdateOptions{
		Command: nil,
	}
	for _, opt := range opts {
		if _, err := opt(&updateOpts); err != nil {
			return err
		}
	}
	if cs.options.ReadOnly {
		return fmt.Errorf("'%s' failed to update command - instance is readonly", cs.String())
	}
	var cmd comby.Command = updateOpts.Command
	if cmd == nil {
		return fmt.Errorf("'%s' failed to update command - command is nil", cs.String())
	}
	if len(cmd.GetCommandUuid()) < 1 {
		return fmt.Errorf("'%s' failed to update command - command uuid is invalid", cs.String())
	}

	// convert to db format
	dbRecord, err := internal.BaseCommandToDbCommand(cmd)
	if err != nil {
		return err
	}

	// encrypt domain data if crypto service is provided
	if cs.options.CryptoService != nil {
		if err := cs.encryptDomainData(dbRecord); err != nil {
			return err
		}
	}

	// sql begin transaction
	tx, err := cs.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	query := `UPDATE commands SET
		instance_id=?,
		tenant_uuid=?,
		domain=?,
		created_at=?,
		data_type=?,
		data_bytes=?,
		req_ctx=?
	 WHERE uuid=?;`

	_, err = tx.ExecContext(ctx,
		query,
		dbRecord.InstanceId,
		dbRecord.TenantUuid,
		dbRecord.Domain,
		dbRecord.CreatedAt,
		dbRecord.DataType,
		dbRecord.DataBytes,
		dbRecord.ReqCtx,
		dbRecord.Uuid)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (cs *commandStoreSQLite) Delete(ctx context.Context, opts ...comby.CommandStoreDeleteOption) error {
	deleteOpts := comby.CommandStoreDeleteOptions{}
	for _, opt := range opts {
		if _, err := opt(&deleteOpts); err != nil {
			return err
		}
	}
	if cs.options.ReadOnly {
		return fmt.Errorf("'%s' failed to delete command - instance is readonly", cs.String())
	}
	var commandUuid string = deleteOpts.CommandUuid
	if len(commandUuid) < 1 {
		return fmt.Errorf("'%s' failed to delete command - command uuid '%s' is invalid", cs.String(), commandUuid)
	}

	_, err := cs.db.ExecContext(ctx, "DELETE FROM commands WHERE uuid=?;", commandUuid)
	return err
}

func (cs *commandStoreSQLite) Total(ctx context.Context) int64 {
	// run query (no args to not using prepared statement)
	row := cs.db.QueryRowContext(ctx, `SELECT COUNT(id) FROM commands;`)
	if err := row.Err(); err != nil {
		return 0
	}
	// extract record
	var dbTotal int64
	if err := row.Scan(&dbTotal); err != nil {
		return 0
	}
	return dbTotal
}

func (cs *commandStoreSQLite) Close(ctx context.Context) error {
	return cs.db.Close()
}
func (cs *commandStoreSQLite) Options() comby.CommandStoreOptions {
	return cs.options
}

func (cs *commandStoreSQLite) String() string {
	return fmt.Sprintf("sqlite - %s", cs.path)
}

func (cs *commandStoreSQLite) Info(ctx context.Context) (*comby.CommandStoreInfoModel, error) {

	row := cs.db.QueryRowContext(ctx, "SELECT COUNT(uuid) FROM commands;")
	if err := row.Err(); err != nil {
		return nil, err
	}
	// extract record
	var dbTotal int64
	if err := row.Scan(&dbTotal); err != nil {
		return nil, err
	}

	row = cs.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(created_at), 0) FROM commands;")
	if err := row.Err(); err != nil {
		return nil, err
	}
	// extract record
	var dbLastCreatedAt int64
	if err := row.Scan(&dbLastCreatedAt); err != nil {
		return nil, err
	}

	return &comby.CommandStoreInfoModel{
		StoreType:         "sqlite",
		LastItemCreatedAt: dbLastCreatedAt,
		NumItems:          dbTotal,
		ConnectionInfo:    cs.path,
	}, nil
}

func (cs *commandStoreSQLite) Reset(ctx context.Context) error {
	if cs.options.ReadOnly {
		return fmt.Errorf("'%s' failed to reset - instance is readonly", cs.String())
	}

	//try to delete all files
	files, err := filepath.Glob(cs.path + "*")
	if err != nil {
		return err
	}
	for _, file := range files {
		err = os.Remove(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *commandStoreSQLite) encryptDomainData(dbRecord *internal.Command) error {
	if cs.options.CryptoService == nil {
		return fmt.Errorf("'%s' failed - crypto service is nil", cs.String())
	}
	domainData := []byte(dbRecord.DataBytes)
	if len(domainData) < 1 {
		return fmt.Errorf("'%s' failed - domain data is empty", cs.String())
	}
	if encryptedData, err := cs.options.CryptoService.Encrypt(domainData); err != nil {
		return fmt.Errorf("'%s' failed - failed to encrypt domain data: %w", cs.String(), err)
	} else {
		dbRecord.DataBytes = hex.EncodeToString(encryptedData)
	}
	return nil
}

func (cs *commandStoreSQLite) decryptDomainData(dbRecord *internal.Command) error {
	if cs.options.CryptoService == nil {
		return fmt.Errorf("'%s' failed - crypto service is nil", cs.String())
	}
	encryptedData, err := hex.DecodeString(dbRecord.DataBytes)
	if err != nil {
		return fmt.Errorf("'%s' failed - failed to decode hex domain data: %w", cs.String(), err)
	}
	if len(encryptedData) < 1 {
		return fmt.Errorf("'%s' failed - encrypted domain data is empty", cs.String())
	}
	if decryptedData, err := cs.options.CryptoService.Decrypt(encryptedData); err != nil {
		return fmt.Errorf("'%s' failed - failed to decrypt domain data: %w", cs.String(), err)
	} else {
		dbRecord.DataBytes = string(decryptedData)
	}
	return nil
}
