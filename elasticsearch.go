package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/exp/slog"
)

// An ElasticSearchAggregateQuerier performs aggregate queries against an elasticsearch index
// The query should be a metric aggregation in the format '"aggregate function": { params }'
// The query should be unmarshable into the ElasticSearchAggregateQueryJSON type
// For example:
//
//	{ "cardinality": {"field": "peer"} }
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics.html
type ElasticSearchAggregateQuerier struct {
	api      string
	index    string
	username string
	password string
}

var _ Querier = (*ElasticSearchAggregateQuerier)(nil)

func NewElasticSearchAggregateQuerier(api string, index string, username string, password string) (*ElasticSearchAggregateQuerier, error) {
	u, err := url.Parse(api)
	if err != nil {
		return nil, fmt.Errorf("invalid api url: %w", err)
	}

	u.Path = fmt.Sprintf("/%s/_search", index)

	return &ElasticSearchAggregateQuerier{
		api:      u.String(),
		index:    index,
		username: username,
		password: password,
	}, nil
}

func (e *ElasticSearchAggregateQuerier) Execute(ctx context.Context, query string, fromTime, toTime time.Time, interval QueryInterval) ([]DataPoint, error) {
	var qry ElasticSearchAggregateQueryJSON
	if err := json.Unmarshal([]byte(query), &qry); err != nil {
		return nil, fmt.Errorf("invalid query %q: %w", query, err)
	}

	var calendarInterval string
	switch interval {
	case QueryIntervalWeekly:
		calendarInterval = "week"
	case QueryIntervalDaily:
		calendarInterval = "day"
	case QueryIntervalHourly:
		calendarInterval = "hour"
	default:
		return nil, fmt.Errorf("unsupported query interval: %q", interval)
	}

	in := &ElasticSearchAggregateRequestJSON{
		Size: 0,
		Query: ElasticSearchAggregateQueryParamsJSON{
			Range: ElasticSearchAggregateRangeJSON{
				Timestamp: ElasticSearchAggregateRangeTimestampJSON{
					Gte: fromTime,
					Lt:  toTime,
				},
			},
		},
		Aggs: map[string]ElasticSearchAggregateAggJSON{
			"A": {
				DateHistogram: ElasticSearchAggregateDateHistogramJSON{
					Field:            "@timestamp",
					CalendarInterval: calendarInterval,
					Order: ElasticSearchAggregateDateHistogramOrderJSON{
						Key: "desc",
					},
				},
				Aggs: map[string]ElasticSearchAggregateQueryJSON{
					"result": qry, // "result" corresponds to result field in ElasticSearchAggregateBucketJSON
				},
			},
		},
	}

	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(in); err != nil {
		return nil, fmt.Errorf("failed to encode query request: %w", err)
	}
	slog.Debug("sending request", "body", buf.String())

	hc := http.Client{}
	req, err := http.NewRequest("POST", e.api, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to create new request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(e.username, e.password)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed: %s", resp.Status)
	}

	defer resp.Body.Close()
	// read body fully so we have it for diagnosis during development
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body request: %w", err)
	}
	slog.Debug("received response", "body", string(body))

	var out ElasticSearchAggregateResponseJSON
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&out); err != nil {
		return nil, fmt.Errorf("failed to decode query response: %w", err)
	}

	if out.TimedOut {
		return nil, fmt.Errorf("query timed out")
	}

	agg, ok := out.Aggregations["A"]
	if !ok {
		return nil, fmt.Errorf(`expected aggregation "A" not found`)
	}

	if len(agg.Buckets) != 1 {
		return nil, fmt.Errorf("unexpected number of aggregation buckets found: %d", len(agg.Buckets))
	}

	bucket := agg.Buckets[0]

	valueTime, err := time.Parse("2006-01-02T15:04:05.999Z", bucket.KeyAsString)
	if err != nil {
		return nil, fmt.Errorf("invalid time in response %q: %w", bucket.KeyAsString, err)
	}

	if !valueTime.Equal(fromTime) {
		return nil, fmt.Errorf("unexpected time in response %q (expected %q)", valueTime.Format("2006-01-02T15:04:05.999Z"), fromTime.Format("2006-01-02T15:04:05.999Z"))
	}

	point := DataPoint{
		// elasticsearch returns the start of the range as the key, but our convention is to use the end time
		Time: toTime,
	}

	switch tv := bucket.Result.Value.(type) {
	case float64:
		point.Value = tv
	case int64:
		point.Value = float64(tv)
	default:
		return nil, fmt.Errorf("unexpected value type in aggregation: %T", bucket.Result.Value)
	}

	return []DataPoint{point}, nil
}

type ElasticSearchAggregateRequestJSON struct {
	Size  int                                      `json:"size"`
	Query ElasticSearchAggregateQueryParamsJSON    `json:"query"`
	Aggs  map[string]ElasticSearchAggregateAggJSON `json:"aggs"`
}

type ElasticSearchAggregateQueryParamsJSON struct {
	Range ElasticSearchAggregateRangeJSON `json:"range"`
}

type ElasticSearchAggregateRangeJSON struct {
	Timestamp ElasticSearchAggregateRangeTimestampJSON `json:"@timestamp"`
}

type ElasticSearchAggregateRangeTimestampJSON struct {
	Gte time.Time `json:"gte,omitempty"`
	Lt  time.Time `json:"lt,omitempty"`
}

type ElasticSearchAggregateAggJSON struct {
	DateHistogram ElasticSearchAggregateDateHistogramJSON    `json:"date_histogram"`
	Aggs          map[string]ElasticSearchAggregateQueryJSON `json:"aggs"`
}

type ElasticSearchAggregateDateHistogramJSON struct {
	Field            string                                       `json:"field"`
	CalendarInterval string                                       `json:"calendar_interval"`
	Order            ElasticSearchAggregateDateHistogramOrderJSON `json:"order"`
}

type ElasticSearchAggregateDateHistogramOrderJSON struct {
	Key string `json:"_key"`
}

type ElasticSearchAggregateQueryJSON struct {
	Cardinality map[string]any `json:"cardinality,omitempty"`
	// TODO: support other aggregate query types such as max/min
	// see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics.html
}

type ElasticSearchAggregateResponseJSON struct {
	TimedOut     bool                                  `json:"timed_out"`
	Aggregations map[string]ElasticSearchAggregateJSON `json:"aggregations"`
}

type ElasticSearchAggregateJSON struct {
	Buckets []ElasticSearchAggregateBucketJSON `json:"buckets"`
}

type ElasticSearchAggregateBucketJSON struct {
	KeyAsString string                           `json:"key_as_string"`
	Key         any                              `json:"key"`
	DocCount    int                              `json:"doc_count"`
	Result      ElasticSearchAggregateResultJSON `json:"result"` // the name of this field is dynamic and set by the input query
}

type ElasticSearchAggregateResultJSON struct {
	Value any `json:"value"`
}
