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
var _ comby.EventStore = (*eventStoreSQLite)(nil)

type eventStoreSQLite struct {
	options comby.EventStoreOptions
	db      *sql.DB

	// sqlite specific options
	path string
}

func NewEventStoreSQLite(path string, opts ...comby.EventStoreOption) comby.EventStore {
	es := &eventStoreSQLite{
		path: path,
	}
	for _, opt := range opts {
		if _, err := opt(&es.options); err != nil {
			return nil
		}
	}
	return es
}

func (es *eventStoreSQLite) connect(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("sqlite", es.path)
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
	if _, err := db.ExecContext(ctx, query); err != nil {
		return nil, err
	}
	return db, nil
}

func (es *eventStoreSQLite) migrate(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS events (id INTEGER,
		instance_id INTEGER,
		uuid TEXT,
		tenant_uuid TEXT,
		command_uuid TEXT,
		domain TEXT,
		aggregate_uuid TEXT,
		version INTEGER,
		created_at INTEGER,
		data_type TEXT,
		data_bytes TEXT,
		PRIMARY KEY (id)
	);
	CREATE INDEX IF NOT EXISTS "tenant_index" ON "events" (
		"tenant_uuid" ASC
	);
	CREATE INDEX IF NOT EXISTS "aggregate_uuid_index" ON "events" (
		"aggregate_uuid" ASC
	);
	`
	_, err := es.db.ExecContext(ctx, query)
	return err
}

// fullfilling EventStore interface
func (es *eventStoreSQLite) Init(ctx context.Context, opts ...comby.EventStoreOption) error {
	for _, opt := range opts {
		if _, err := opt(&es.options); err != nil {
			return err
		}
	}

	// connect to db (or create new one)
	if db, err := es.connect(ctx); err != nil {
		return err
	} else {
		es.db = db
	}

	// auto-migrate table
	if !es.options.ReadOnly {
		if err := es.migrate(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (es *eventStoreSQLite) Create(ctx context.Context, opts ...comby.EventStoreCreateOption) error {
	createOpts := comby.EventStoreCreateOptions{
		Event: nil,
	}
	for _, opt := range opts {
		if _, err := opt(&createOpts); err != nil {
			return err
		}
	}

	if es.options.ReadOnly {
		return fmt.Errorf("'%s' failed to create event - instance is readonly", es.String())
	}

	var evt comby.Event = createOpts.Event
	if evt == nil {
		return fmt.Errorf("'%s' failed to create event - event is nil", es.String())
	}
	if len(evt.GetEventUuid()) < 1 {
		return fmt.Errorf("'%s' failed to create event - event uuid is invalid", es.String())
	}

	// sql statement
	dbRecord, err := internal.BaseEventToDbEvent(evt)
	if err != nil {
		return err
	}

	// encrypt domain data if crypto service is provided
	if es.options.CryptoService != nil {
		if err := es.encryptDomainData(dbRecord); err != nil {
			return err
		}
	}

	// sql begin transaction
	tx, err := es.db.Begin()
	if err != nil {
		return err
	}

	// prepare statement
	query := `INSERT INTO events (
	instance_id, 
	uuid, 
	tenant_uuid,
	command_uuid,
	domain,
	aggregate_uuid,
	version,
	created_at,
	data_type,
	data_bytes
) VALUES (?,?,?,?,?,?,?,?,?,?);`
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
		dbRecord.CommandUuid,
		dbRecord.Domain,
		dbRecord.AggregateUuid,
		dbRecord.Version,
		dbRecord.CreatedAt,
		dbRecord.DataType,
		dbRecord.DataBytes,
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

func (es *eventStoreSQLite) Get(ctx context.Context, opts ...comby.EventStoreGetOption) (comby.Event, error) {
	getOpts := comby.EventStoreGetOptions{}
	for _, opt := range opts {
		if _, err := opt(&getOpts); err != nil {
			return nil, err
		}
	}

	// prepare query
	var query string = "SELECT * FROM events LIMIT 1;"
	if len(getOpts.EventUuid) > 0 {
		query = fmt.Sprintf("SELECT * FROM events WHERE uuid='%s' LIMIT 1;", getOpts.EventUuid)
	}

	// run query (no args to not using prepared statement)
	row := es.db.QueryRowContext(ctx, query)
	if row.Err() != nil {
		return nil, row.Err()
	}

	// extract record
	var dbRecord internal.Event
	if err := row.Scan(
		&dbRecord.ID,
		&dbRecord.InstanceId,
		&dbRecord.Uuid,
		&dbRecord.TenantUuid,
		&dbRecord.CommandUuid,
		&dbRecord.Domain,
		&dbRecord.AggregateUuid,
		&dbRecord.Version,
		&dbRecord.CreatedAt,
		&dbRecord.DataType,
		&dbRecord.DataBytes,
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
	if es.options.CryptoService != nil {
		if err := es.decryptDomainData(&dbRecord); err != nil {
			return nil, err
		}
	}

	// db record to event
	evt, err := internal.DbEventToBaseEvent(&dbRecord)
	if err != nil {
		return nil, err
	}

	return evt, err
}

func (es *eventStoreSQLite) List(ctx context.Context, opts ...comby.EventStoreListOption) ([]comby.Event, int64, error) {
	listOpts := comby.EventStoreListOptions{
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
	if len(listOpts.AggregateUuid) > 0 {
		whereList = append(whereList, fmt.Sprintf("aggregate_uuid='%s'", listOpts.AggregateUuid))
	}
	if len(listOpts.DataType) > 0 {
		whereList = append(whereList, fmt.Sprintf("data_type='%s'", listOpts.DataType))
	}
	if len(listOpts.Domains) > 0 {
		inStr := ""
		for index, _domain := range listOpts.Domains {
			inStr += fmt.Sprintf("'%s'", _domain)
			if len(listOpts.Domains) > 1 && index < len(listOpts.Domains)-1 {
				inStr = fmt.Sprintf("%s, ", inStr)
			}
		}
		stmt := fmt.Sprintf("domain IN (%s)", inStr)
		whereList = append(whereList, stmt)
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
	var queryTotalQuery string = fmt.Sprintf("SELECT COUNT(id) FROM events%s;", whereSQL)
	row := es.db.QueryRowContext(ctx, queryTotalQuery)
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

	// prepare limit/offset statements
	var limitSQL string = ""
	var offsetSQL string = ""
	if listOpts.Limit >= 0 {
		limitSQL = fmt.Sprintf(" LIMIT %d", listOpts.Limit)
	}
	if listOpts.Offset >= 0 {
		offsetSQL = fmt.Sprintf(" OFFSET %d", listOpts.Offset)
	}

	// run query (no args to not using prepared statement - see above for more info)
	var query string = fmt.Sprintf("SELECT * FROM events%s%s%s%s;", whereSQL, orderBySQL, limitSQL, offsetSQL)
	rows, err := es.db.QueryContext(ctx, query)
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
	var dbRecords []*internal.Event
	for rows.Next() {
		var dbRecord internal.Event
		if err := rows.Scan(
			&dbRecord.ID,
			&dbRecord.InstanceId,
			&dbRecord.Uuid,
			&dbRecord.TenantUuid,
			&dbRecord.CommandUuid,
			&dbRecord.Domain,
			&dbRecord.AggregateUuid,
			&dbRecord.Version,
			&dbRecord.CreatedAt,
			&dbRecord.DataType,
			&dbRecord.DataBytes,
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
	if es.options.CryptoService != nil {
		for _, dbRecord := range dbRecords {
			if err := es.decryptDomainData(dbRecord); err != nil {
				return nil, 0, err
			}
		}
	}

	// convert
	evts, err := internal.DbEventsToBaseEvents(dbRecords)
	if err != nil {
		return nil, 0, err
	}
	return evts, queryTotal, err
}

func (es *eventStoreSQLite) Update(ctx context.Context, opts ...comby.EventStoreUpdateOption) error {
	updateOpts := comby.EventStoreUpdateOptions{
		Event: nil,
	}
	for _, opt := range opts {
		if _, err := opt(&updateOpts); err != nil {
			return err
		}
	}
	if es.options.ReadOnly {
		return fmt.Errorf("'%s' failed to update event - instance is readonly", es.String())
	}

	var evt comby.Event = updateOpts.Event
	if evt == nil {
		return fmt.Errorf("'%s' failed to update event - event is nil", es.String())
	}
	if len(evt.GetEventUuid()) < 1 {
		return fmt.Errorf("'%s' failed to update event - event uuid is invalid", es.String())
	}

	// convert to db format
	dbRecord, err := internal.BaseEventToDbEvent(evt)
	if err != nil {
		return err
	}

	// encrypt domain data if crypto service is provided
	if es.options.CryptoService != nil {
		if err := es.encryptDomainData(dbRecord); err != nil {
			return err
		}
	}

	// sql begin transaction
	tx, err := es.db.Begin()
	if err != nil {
		return err
	}

	// prepare statement
	query := `UPDATE events SET
		instance_id=?, 
		tenant_uuid=?,
		command_uuid=?,
		domain=?,
		aggregate_uuid=?,
		version=?,
		created_at=?,
		data_type=?,
		data_bytes=?
	 WHERE uuid=?;`
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	// execute statement
	_, err = stmt.ExecContext(ctx,
		dbRecord.InstanceId,
		dbRecord.TenantUuid,
		dbRecord.CommandUuid,
		dbRecord.Domain,
		dbRecord.AggregateUuid,
		dbRecord.Version,
		dbRecord.CreatedAt,
		dbRecord.DataType,
		dbRecord.DataBytes,
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

func (es *eventStoreSQLite) Delete(ctx context.Context, opts ...comby.EventStoreDeleteOption) error {
	deleteOpts := comby.EventStoreDeleteOptions{}
	for _, opt := range opts {
		if _, err := opt(&deleteOpts); err != nil {
			return err
		}
	}
	if es.options.ReadOnly {
		return fmt.Errorf("'%s' failed to delete event - instance is readonly", es.String())
	}

	var eventUuid string = deleteOpts.EventUuid
	if len(eventUuid) < 1 {
		return fmt.Errorf("'%s' failed to delete event - event uuid '%s' is invalid", es.String(), eventUuid)
	}

	// run query (no args to not using prepared statement)
	query := fmt.Sprintf("DELETE FROM events WHERE uuid='%s';", eventUuid)
	_, err := es.db.ExecContext(ctx, query)
	return err
}

func (es *eventStoreSQLite) Total(ctx context.Context) int64 {
	// run query (no args to not using prepared statement)
	row := es.db.QueryRowContext(ctx, `SELECT COUNT(id) FROM events;`)
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

func (es *eventStoreSQLite) UniqueList(ctx context.Context, opts ...comby.EventStoreUniqueListOption) ([]string, int64, error) {
	listOpts := comby.EventStoreUniqueListOptions{
		DbField:   "tenant_uuid",
		Offset:    0,
		Limit:     100,
		Ascending: true,
	}
	for _, opt := range opts {
		if _, err := opt(&listOpts); err != nil {
			return nil, 0, err
		}
	}

	// prepare where
	var whereSQL string = ""
	var whereList []string = []string{}
	if len(listOpts.TenantUuid) > 0 {
		whereList = append(whereList, fmt.Sprintf("tenant_uuid='%s'", listOpts.TenantUuid))
	}
	if len(listOpts.Domain) > 0 {
		whereList = append(whereList, fmt.Sprintf("domain='%s'", listOpts.Domain))
	}

	// note the first empty character(s) below
	for index, where := range whereList {
		if index == 0 {
			whereSQL = fmt.Sprintf(" WHERE %s", where)
		} else {
			whereSQL = fmt.Sprintf("%s AND %s", whereSQL, where)
		}
	}

	// prepare orderby
	var orderBySQL string = ""
	if len(listOpts.DbField) > 0 {
		if listOpts.Ascending {
			orderBySQL = fmt.Sprintf(" ORDER BY %s ASC", listOpts.DbField)
		} else {
			orderBySQL = fmt.Sprintf(" ORDER BY %s DESC", listOpts.DbField)
		}
	}

	// prepare limit/offset statements
	var limitSQL string = ""
	var offsetSQL string = ""
	if listOpts.Limit >= 0 {
		limitSQL = fmt.Sprintf(" LIMIT %d", listOpts.Limit)
	}
	if listOpts.Offset >= 0 {
		offsetSQL = fmt.Sprintf(" OFFSET %d", listOpts.Offset)
	}

	// run query (no args to not using prepared statement)
	var query string = fmt.Sprintf("SELECT DISTINCT %s FROM events%s%s%s%s;", listOpts.DbField, whereSQL, orderBySQL, limitSQL, offsetSQL)
	rows, err := es.db.QueryContext(ctx, query)
	switch {
	case err == sql.ErrNoRows:
		return nil, 0, nil
	case err != nil:
		return nil, 0, err
	}
	if rows != nil {
		defer rows.Close()
	}

	// extract results
	var dbUniqueValues []string
	for rows.Next() {
		var dbUniqueValue string
		if err := rows.Scan(&dbUniqueValue); err != nil {
			return nil, 0, err
		}
		dbUniqueValues = append(dbUniqueValues, dbUniqueValue)
	}
	if err := rows.Close(); err != nil {
		return nil, 0, err
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	// run extra total query (no args to not using prepared statement)
	var totalQuery string = fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM events%s;", listOpts.DbField, whereSQL)
	row := es.db.QueryRowContext(ctx, totalQuery)
	if err := row.Err(); err != nil {
		return nil, 0, err
	}
	// extract record
	var dbTotal int64
	if err := row.Scan(&dbTotal); err != nil {
		return nil, 0, err
	}

	return dbUniqueValues, dbTotal, nil
}

func (es *eventStoreSQLite) Close(ctx context.Context) error {
	return es.db.Close()
}

func (es *eventStoreSQLite) Options() comby.EventStoreOptions {
	return es.options
}

func (es *eventStoreSQLite) String() string {
	return fmt.Sprintf("sqlite - %s", es.path)
}

func (es *eventStoreSQLite) Info(ctx context.Context) (*comby.EventStoreInfoModel, error) {

	// run extra total query (no args to not using prepared statement)
	var totalQuery string = fmt.Sprintf("SELECT COUNT(uuid) FROM events;")
	row := es.db.QueryRowContext(ctx, totalQuery)
	if err := row.Err(); err != nil {
		return nil, err
	}
	// extract record
	var dbTotal int64
	if err := row.Scan(&dbTotal); err != nil {
		return nil, err
	}

	// run extra total query (no args to not using prepared statement)
	var lastEventQuery string = fmt.Sprintf("SELECT COALESCE(MAX(created_at), 0) FROM events;")
	row = es.db.QueryRowContext(ctx, lastEventQuery)
	if err := row.Err(); err != nil {
		return nil, err
	}
	// extract record
	var dbLastCreatedAt int64
	if err := row.Scan(&dbLastCreatedAt); err != nil {
		return nil, err
	}

	return &comby.EventStoreInfoModel{
		StoreType:         "sqlite",
		LastItemCreatedAt: dbLastCreatedAt,
		NumItems:          dbTotal,
		ConnectionInfo:    es.path,
	}, nil
}

func (es *eventStoreSQLite) Reset(ctx context.Context) error {
	if es.options.ReadOnly {
		return fmt.Errorf("'%s' failed to reset - instance is readonly", es.String())
	}

	//try to delete all files
	files, err := filepath.Glob(es.path + "*")
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

func (es *eventStoreSQLite) encryptDomainData(dbRecord *internal.Event) error {
	if es.options.CryptoService == nil {
		return fmt.Errorf("'%s' failed - crypto service is nil", es.String())
	}
	domainData := []byte(dbRecord.DataBytes)
	if len(domainData) < 1 {
		return fmt.Errorf("'%s' failed - domain data is empty", es.String())
	}
	if encryptedData, err := es.options.CryptoService.Encrypt(domainData); err != nil {
		return fmt.Errorf("'%s' failed - failed to encrypt domain data: %w", es.String(), err)
	} else {
		dbRecord.DataBytes = hex.EncodeToString(encryptedData)
	}
	return nil
}

func (es *eventStoreSQLite) decryptDomainData(dbRecord *internal.Event) error {
	if es.options.CryptoService == nil {
		return fmt.Errorf("'%s' failed - crypto service is nil", es.String())
	}
	encryptedData, err := hex.DecodeString(dbRecord.DataBytes)
	if err != nil {
		return fmt.Errorf("'%s' failed - failed to decode hex domain data: %w", es.String(), err)
	}
	if len(encryptedData) < 1 {
		return fmt.Errorf("'%s' failed - encrypted domain data is empty", es.String())
	}
	if decryptedData, err := es.options.CryptoService.Decrypt(encryptedData); err != nil {
		return fmt.Errorf("'%s' failed - failed to decrypt domain data: %w", es.String(), err)
	} else {
		dbRecord.DataBytes = string(decryptedData)
	}
	return nil
}
