package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gradientzero/comby/v2"
	combyStore "github.com/gradientzero/comby/v2/store"
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

func NewCommandStoreSQLite(path string) comby.CommandStore {
	return &commandStoreSQLite{
		path: path,
	}
}

func (cs *commandStoreSQLite) connect(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("sqlite", cs.path)
	if err != nil {
		return nil, err
	}
	// golang database/sql driver handles internally a connection pool
	// unfortunately, sqlite is not thread-safe. So we need to limit
	// the number of open connections to 1
	// Background:
	// 1 write goroutine and many read goroutine are allowed,
	// but tests showed that 1 write and many read goroutine are also not working
	// so we need to limit the number of open connections to 1
	db.SetMaxOpenConns(1)

	// set sqlite specific pragmas
	query := `
		PRAGMA journal_mode=DELETE;
		PRAGMA synchronous=FULL;
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
	dbRecord, err := combyStore.BaseCommandToDbCommand(cmd)
	if err != nil {
		return err
	}

	// sql begin transaction
	tx, err := cs.db.Begin()
	if err != nil {
		return err
	}

	// prepare statement
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
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	// execute statement
	_, err = stmt.ExecContext(
		ctx,
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

	// close statement
	err = stmt.Close()
	if err != nil {
		return err
	}

	// commit statement
	return tx.Commit()
}

func (cs *commandStoreSQLite) Get(ctx context.Context, opts ...comby.CommandStoreGetOption) (comby.Command, error) {
	getOpts := comby.CommandStoreGetOptions{}
	for _, opt := range opts {
		if _, err := opt(&getOpts); err != nil {
			return nil, err
		}
	}

	// prepare query
	var query string = "SELECT * FROM commands LIMIT 1;"
	if len(getOpts.CommandUuid) > 0 {
		query = fmt.Sprintf("SELECT * FROM commands WHERE uuid='%s' LIMIT 1;", getOpts.CommandUuid)
	}

	// run query (no args to not using prepared statement)
	row := cs.db.QueryRowContext(ctx, query)
	if row.Err() != nil {
		return nil, row.Err()
	}

	// extract record
	var dbRecord combyStore.Command
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

	// db record to command
	cmd, err := combyStore.DbCommandToBaseCommand(&dbRecord)
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

	// prepare statement: (do NOT used them for Query/QueryContext)
	// 1. see different syntax for postgres:
	// http://go-database-sql.org/prepared.html#parameter-placeholder-syntax
	// 2. db.Query and db.QueryContext for some reason it does not work as expected
	// (seems to be something internally in database/sql because for SQLite and Postgres
	// simply does not return the expected result after sending new values to prepared statement)
	var whereSQL string = ""
	var whereList []string = []string{}
	if len(listOpts.TenantUuid) > 0 {
		whereList = append(whereList, fmt.Sprintf("tenant_uuid='%s'", listOpts.TenantUuid))
	}
	if len(listOpts.Domain) > 0 {
		whereList = append(whereList, fmt.Sprintf("domain='%s'", listOpts.Domain))
	}
	if len(listOpts.DataType) > 0 {
		whereList = append(whereList, fmt.Sprintf("data_type='%s'", listOpts.DataType))
	}
	if listOpts.Before >= 0 {
		whereList = append(whereList, fmt.Sprintf("created_at<%d", listOpts.Before))
	}
	if listOpts.After >= 0 {
		whereList = append(whereList, fmt.Sprintf("created_at>%d", listOpts.After))
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
	row := cs.db.QueryRowContext(ctx, queryTotalQuery)
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

	// run query (no args to not using prepared statement - see above for more info)
	var query string = fmt.Sprintf("SELECT * FROM commands%s%s%s%s;", whereSQL, orderBySQL, limitSQL, offsetSQL)
	rows, err := cs.db.QueryContext(ctx, query)
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
	var dbRecords []*combyStore.Command
	for rows.Next() {
		var dbRecord combyStore.Command
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

	// convert
	cmds, err := combyStore.DbCommandsToBaseCommands(dbRecords)
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
	dbRecord, err := combyStore.BaseCommandToDbCommand(cmd)
	if err != nil {
		return err
	}

	// sql begin transaction
	tx, err := cs.db.Begin()
	if err != nil {
		return err
	}

	// prepare statement
	query := `UPDATE commands SET
		instance_id=?, 
		tenant_uuid=?,
		domain=?,
		created_at=?,
		data_type=?,
		data_bytes=?,
		req_ctx=?
	) WHERE uuid=?;`
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	// execute statement
	_, err = stmt.ExecContext(ctx,
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

	// close statement
	err = stmt.Close()
	if err != nil {
		return err
	}

	// commit statement
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

	// run query (no args to not using prepared statement)
	query := fmt.Sprintf("DELETE FROM commands WHERE uuid='%s';", commandUuid)
	_, err := cs.db.ExecContext(ctx, query)
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

	// run extra total query (no args to not using prepared statement)
	var totalQuery string = fmt.Sprintf("SELECT COUNT(uuid) FROM commands;")
	row := cs.db.QueryRowContext(ctx, totalQuery)
	if err := row.Err(); err != nil {
		return nil, err
	}
	// extract record
	var dbTotal int64
	if err := row.Scan(&dbTotal); err != nil {
		return nil, err
	}

	// run extra total query (no args to not using prepared statement)
	var lastEventQuery string = fmt.Sprintf("SELECT MAX(created_at) FROM commands;")
	row = cs.db.QueryRowContext(ctx, lastEventQuery)
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
