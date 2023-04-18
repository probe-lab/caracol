package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type QueryInterval string

func (q QueryInterval) String() string { return string(q) }

const (
	QueryIntervalHourly QueryInterval = "hourly" // query represents an hour of data
	QueryIntervalDaily  QueryInterval = "daily"  // query represents a day of data
	QueryIntervalWeekly QueryInterval = "weekly" // query represents a week of data
)

// WARNING: don't change field order since it is used when populating from database
type Query struct {
	ID         int
	Name       string
	Query      string
	Interval   QueryInterval
	Start      time.Time
	Finish     *time.Time
	QueryType  QueryType
	Dataset    string
	ProviderID int
	ApiType    ApiType
	ApiURL     string
	AuthType   AuthType
}

func (q *Query) SeqTime(seq int) time.Time {
	switch q.Interval {
	case QueryIntervalHourly:
		return q.Start.Add(time.Duration(seq) * time.Hour).UTC()
	case QueryIntervalDaily:
		return q.Start.Add(time.Duration(seq) * time.Hour * 24).UTC()
	case QueryIntervalWeekly:
		return q.Start.Add(time.Duration(seq) * time.Hour * 24 * 7).UTC()
	default:
		return time.Time{}.UTC()
	}
}

// SeqAfter returns the next sequence number after the specified time
// t must not be before the start of the query
func (q *Query) SeqAfter(t time.Time) int {
	sinceStart := t.Sub(q.Start)
	switch q.Interval {
	case QueryIntervalHourly:
		return 1 + int(sinceStart/time.Hour)
	case QueryIntervalDaily:
		return 1 + int(sinceStart/(24*time.Hour))
	case QueryIntervalWeekly:
		return 1 + int(sinceStart/(7*24*time.Hour))
	default:
		return -1
	}
}

type ApiType string

func (t ApiType) String() string { return string(t) }

const (
	ApiTypeGrafanaCloud  ApiType = "grafanacloud"
	ApiTypeElasticSearch ApiType = "elasticsearch"
)

type AuthType string

func (t AuthType) String() string { return string(t) }

const (
	AuthTypeBearerToken AuthType = "bearer_token"
	AuthTypeBasicAuth   AuthType = "basic_auth"
)

type QueryType string

func (t QueryType) String() string { return string(t) }

const (
	QueryTypePrometheus             QueryType = "prometheus"
	QueryTypeElasticSearchAggregate QueryType = "elasticsearch_aggregate"
)

// WARNING: don't change field order since it is used when populating from database
type Source struct {
	ID         int
	Name       string
	Dataset    string
	ProviderID int
	ApiType    ApiType
	ApiURL     string
	AuthType   AuthType
}

type SecretType string

func (t SecretType) String() string { return string(t) }

const (
	SecretTypeBearerToken SecretType = "bearer_token"
	SecretTypeUsername    SecretType = "username"
	SecretTypePassword    SecretType = "password"
)

type DataPoint struct {
	Seq   int
	Time  time.Time
	Value float64
}

type Querier interface {
	Execute(ctx context.Context, query string, fromTime, toTime time.Time, interval QueryInterval) ([]DataPoint, error)
}

func GetQuery(ctx context.Context, db *DB, queryID int) (*Query, error) {
	conn, err := db.NewConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, "select q.id, q.name, q.query, q.interval, q.start, q.finish, q.query_type, s.dataset, p.id, p.api_type, p.api_url, p.auth_type from queries q join sources s on s.id=q.source_id join providers p on p.id=s.provider_id where q.id=$1", queryID)
	if err != nil {
		return nil, fmt.Errorf("select query: %w", err)
	}
	defer rows.Close()

	qry, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByPos[Query])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("collect: %w", err)
	}

	return qry, nil
}

func GetSource(ctx context.Context, db *DB, sourceID int) (*Source, error) {
	conn, err := db.NewConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, "select s.id, s.name, s.dataset, p.id, p.api_type, p.api_url, p.auth_type from sources s join providers p on p.id=s.provider_id where s.id=$1", sourceID)
	if err != nil {
		return nil, fmt.Errorf("select source: %w", err)
	}
	defer rows.Close()

	qry, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByPos[Source])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("collect: %w", err)
	}

	return qry, nil
}

func FetchActiveQueries(ctx context.Context, db *DB) ([]*Query, error) {
	conn, err := db.NewConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, "select q.id, q.name, q.query, q.interval, q.start, NULL, q.query_type, s.dataset, p.id, p.api_type, p.api_url, p.auth_type from queries q join sources s on s.id=q.source_id join providers p on p.id=s.provider_id where (q.finish is null or q.finish > now())")
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	qs, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[Query])
	if err != nil {
		return nil, fmt.Errorf("collect rows: %w", err)
	}

	return qs, nil
}

func FindCollectionGaps(ctx context.Context, db *DB, queryID int) ([]int, error) {
	conn, err := db.NewConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Release()

	sql := `with q as (
			  select start, case
			    when interval='hourly' then extract('hour' from $2-start)::integer
			    when interval='daily'  then extract('day' from $2-start)::integer
			    when interval='weekly' then extract('day' from $2-start)::integer/7
			    else 0
			  end as last
			  from queries where id=$1
			)
			select expected as seq
			from q, generate_series(0, q.last, 1) expected
			left join collections c on expected = c.seq and c.query_id=$1
			where c.seq is null;`

	rows, err := conn.Query(ctx, sql, queryID, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	seqs, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		return nil, fmt.Errorf("collect rows: %w", err)
	}

	return seqs, nil
}

func WriteCollectionSeq(ctx context.Context, db *DB, queryID int, seq int, value float64, force bool) error {
	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	sql := "insert into collections(query_id,seq,value) values ($1,$2,$3)"
	if force {
		sql += " on conflict(query_id,seq) do update set value=excluded.value"
	}

	_, err = tx.Exec(ctx, sql, queryID, seq, value)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func GetEnumValues(ctx context.Context, db *DB, name string) ([]string, error) {
	conn, err := db.NewConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, fmt.Sprintf("select unnest(enum_range(NULL::%s));", name))
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	types, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("collect: %w", err)
	}

	return types, nil
}

func ValidateEnumValue(ctx context.Context, db *DB, enumName string, value string) error {
	types, err := GetEnumValues(ctx, db, enumName)
	if err != nil {
		return fmt.Errorf("get enum values: %w", err)
	}
	found := false
	for _, t := range types {
		if t == value {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("must be one of '%s'", strings.Join(types, "','"))
	}

	return nil
}
