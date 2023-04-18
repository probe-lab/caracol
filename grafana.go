package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/exp/slog"
)

type GrafanaQueryRequestInJSON struct {
	Queries []any  `json:"queries"`
	From    string `json:"from"`
	To      string `json:"to"`
}

type GrafanaPrometheusQueryJSON struct {
	RefID         string                     `json:"refId"`
	Expression    string                     `json:"expr"`
	Format        string                     `json:"format"` // time_series or table
	Range         bool                       `json:"range"`
	Instant       bool                       `json:"instant"`
	Datasource    GrafanaQueryDatasourceJSON `json:"datasource"`
	MaxDataPoints int                        `json:"maxDataPoints"`
	Interval      string                     `json:"interval"`
	IntervalMs    int                        `json:"intervalMs,omitempty"`
}

type GrafanaQueryDatasourceJSON struct {
	UID string `json:"uid"`
}

type GrafanaQueryRequestOutJSON struct {
	Results map[string]GrafanaResultJSON `json:"results"`
}

type GrafanaResultJSON struct {
	Status int                `json:"status"`
	Frames []GrafanaFrameJSON `json:"frames"`
}

type GrafanaFrameJSON struct {
	Schema any             `json:"schema"`
	Data   GrafanaDataJSON `json:"data"`
}

type GrafanaDataJSON struct {
	Values [2][]float64 `json:"values"`
}

type GrafanaCloudQuerier struct {
	api         string
	dsuid       string
	dstype      string
	bearerToken string
}

var _ Querier = (*GrafanaCloudQuerier)(nil)

func NewGrafanaCloudQuerier(api string, dsuid string, dstype QueryType, bearerToken string) (*GrafanaCloudQuerier, error) {
	u, err := url.Parse(api)
	if err != nil {
		return nil, fmt.Errorf("invalid api url: %w", err)
	}

	u.Path = "/api/ds/query"

	return &GrafanaCloudQuerier{
		api:         u.String(),
		dsuid:       dsuid,
		dstype:      string(dstype),
		bearerToken: bearerToken,
	}, nil
}

func (g *GrafanaCloudQuerier) Execute(ctx context.Context, query string, fromTime, toTime time.Time, interval QueryInterval) ([]DataPoint, error) {
	fromTime = fromTime.Add(1)
	var intervalStr string
	var maxPoints int
	switch interval {
	case QueryIntervalHourly:
		intervalStr = "1h"
		maxPoints = int(toTime.Sub(fromTime)/time.Hour) + 1
	case QueryIntervalDaily:
		intervalStr = "1d"
		maxPoints = int(toTime.Sub(fromTime)/(24*time.Hour)) + 1
	default:
		return nil, fmt.Errorf("unsupported query interval: %q", interval)
	}

	slog.Debug("executing grafana query", "uid", g.dsuid, "type", g.dstype, "query", query, "from", fromTime, "to", toTime)

	q := GrafanaQueryRequestInJSON{
		Queries: []any{
			GrafanaPrometheusQueryJSON{
				RefID:         "A",
				Expression:    query,
				Instant:       true,
				Format:        "table",
				Datasource:    GrafanaQueryDatasourceJSON{UID: g.dsuid},
				MaxDataPoints: maxPoints,
				Interval:      intervalStr,
			},
		},
		From: strconv.FormatInt(fromTime.Unix()*1000, 10), // milliseconds
		To:   strconv.FormatInt(toTime.Unix()*1000, 10),   // milliseconds
	}

	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(q); err != nil {
		return nil, fmt.Errorf("failed to encode query request: %w", err)
	}

	slog.Debug("sending request", "body", buf.String())

	hc := http.Client{}
	req, err := http.NewRequest("POST", g.api, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to create new request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", g.bearerToken))

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

	var out GrafanaQueryRequestOutJSON
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&out); err != nil {
		return nil, fmt.Errorf("failed to decode query response: %w", err)
	}

	values := out.Results["A"].Frames[0].Data.Values

	points := make([]DataPoint, len(values[0]))

	for i := range values[0] {
		points[i] = DataPoint{
			Time:  time.Unix(0, int64(values[0][i])*1e6).UTC(),
			Value: values[1][i],
		}
	}

	return points, nil
}
