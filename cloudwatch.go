package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

type CloudWatchQuerier struct {
	client *cloudwatch.Client
}

var _ Querier = (*CloudWatchQuerier)(nil)

func NewCloudWatchQuerier(ctx context.Context, region string, accessKeyID string, secretAccessKey string) (*CloudWatchQuerier, error) {
	credProv := credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credProv),
	)
	if err != nil {
		return nil, err
	}

	client := cloudwatch.NewFromConfig(cfg)

	return &CloudWatchQuerier{client: client}, nil
}

type CloudWatchQuery struct {
	*types.Metric
	Stat string
}

func (c *CloudWatchQuerier) Execute(ctx context.Context, queryJSON string, fromTime, toTime time.Time, interval QueryInterval) ([]DataPoint, error) {
	query := &CloudWatchQuery{}
	if err := json.Unmarshal([]byte(queryJSON), query); err != nil {
		return nil, err
	}

	var period int32
	switch interval {
	case QueryIntervalHourly:
		period = 3600
	case QueryIntervalDaily:
		period = 86400
	case QueryIntervalWeekly:
		period = 604800
	}

	metricDataQuery := types.MetricDataQuery{
		Id: aws.String("caracolrequest"),
		MetricStat: &types.MetricStat{
			Metric: query.Metric,
			Period: aws.Int32(period), // Period in seconds
			Stat:   aws.String(query.Stat),
		},
		ReturnData: aws.Bool(true),
	}

	// Call the GetMetricData API
	params := &cloudwatch.GetMetricDataInput{
		MetricDataQueries: []types.MetricDataQuery{metricDataQuery},
		StartTime:         aws.Time(fromTime),
		EndTime:           aws.Time(toTime),
	}
	output, err := c.client.GetMetricData(ctx, params)
	if err != nil {
		return nil, err
	}

	if len(output.MetricDataResults) != 1 {
		return nil, fmt.Errorf("expected 1 result, got %d", len(output.MetricDataResults))
	}

	result := output.MetricDataResults[0]

	dataPoints := make([]DataPoint, len(result.Values))
	for i, ts := range result.Timestamps {
		// cloudwatch returns the start of the range as the key, but our convention is to use the end time

		if i < len(result.Timestamps)-1 {
			ts = result.Timestamps[i+1]
		} else {
			ts = ts.Add(time.Second * time.Duration(period))
		}

		truncate := toTime.Truncate(time.Minute)
		if truncate.Equal(ts) {
			ts = toTime
		}
		dataPoints[i] = DataPoint{
			Time:  ts,
			Value: result.Values[i],
		}
	}

	return dataPoints, nil
}
