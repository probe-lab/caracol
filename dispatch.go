package main

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"golang.org/x/exp/slog"
)

func DispatchQuery(ctx context.Context, qry *Query, seq int, ps ProviderSecrets) ([]DataPoint, error) {
	logger := slog.With("query_id", qry.ID, "query", qry.Name)

	start := qry.Start.UTC()
	var fromTime time.Time
	var toTime time.Time

	switch qry.Interval {
	case QueryIntervalHourly:
		fromTime = start.Add(time.Duration(seq-1) * time.Hour)
		toTime = fromTime.Add(time.Hour)
	case QueryIntervalDaily:
		fromTime = start.Add(time.Duration(seq-1) * time.Hour * 24)
		toTime = fromTime.Add(time.Hour * 24)
	case QueryIntervalWeekly:
		fromTime = start.Add(time.Duration(seq-1) * time.Hour * 24 * 7)
		toTime = fromTime.Add(time.Hour * 24 * 7)
	default:
		return nil, fmt.Errorf("unsupported query interval: %q", qry.Interval)
	}

	var querier Querier
	switch qry.ApiType {
	case ApiTypeGrafanaCloud:
		var err error
		querier, err = NewGrafanaCloudQuerier(qry.ApiURL, qry.Dataset, qry.QueryType, ps[SecretTypeBearerToken])
		if err != nil {
			return nil, fmt.Errorf("grafanacloud querier: %w", err)
		}
	case ApiTypeElasticSearch:
		switch qry.QueryType {
		case QueryTypeElasticSearchAggregate:
			var err error
			querier, err = NewElasticSearchAggregateQuerier(qry.ApiURL, qry.Dataset, ps[SecretTypeUsername], ps[SecretTypePassword])
			if err != nil {
				return nil, fmt.Errorf("grafanacloud querier: %w", err)
			}

		default:
			return nil, fmt.Errorf("unsupported collection type: %q", qry.ApiType)

		}
	case ApiTypeCloudWatch:
		var err error
		querier, err = NewCloudWatchQuerier(ctx, ps[SecretTypeRegion], ps[SecretTypeAccessKeyID], ps[SecretTypeSecretAccessKey])
		if err != nil {
			return nil, fmt.Errorf("cloudwatch querier: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported datasource type: %q", qry.ApiType)
	}

	// case QueryIntervalWeek:
	// 	fromTime = StartOfWeek(fromTime)
	// 	toTime = StartOfWeek(toTime)
	// default:
	// 	return nil, fmt.Errorf("unsupported interval: %q", qry.AggregateInterval)
	// }

	logger.Info("executing query", "from", fromTime.Format("2006-01-02T15:04:05Z"), "to", toTime.Format("2006-01-02T15:04:05Z"))
	points, err := querier.Execute(ctx, qry.Query, fromTime, toTime, qry.Interval)
	if err != nil {
		return nil, fmt.Errorf("source execute: %w", err)
	}

	// We may get more points than needed depending on the query capabilities
	for _, pt := range points {
		logger.Debug("received data point", "time", pt.Time.Format("2006-01-02T15:04:05Z"), "value", pt.Value)
		if pt.Time.Equal(toTime) {
			return []DataPoint{
				{
					Seq:   seq,
					Time:  pt.Time,
					Value: pt.Value,
				},
			}, nil
		}
	}

	logger.Warn("query did not return expected data point", "seq", seq, "time", toTime.Format("2006-01-02T15:04:05Z"))

	return []DataPoint{}, nil
}

func formatFloat64(v float64) string {
	abs := math.Abs(v)
	if abs == 0 || 1e-6 <= v && v < 1e21 {
		return strconv.FormatFloat(v, 'f', -1, 64)
	} else {
		return fmt.Sprintf("%e", v)
	}
}
