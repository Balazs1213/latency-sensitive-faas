package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"lsf-configurator/pkg/core"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/indices/create"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
)

const (
	metricsIndex = ".ds-metrics-apm.*"
	tracesIndex  = ".ds-traces-apm*"
)

type ApiKeyConfig struct {
	Id                  string `json:"id"`
	Name                string `json:"name"`
	ApiKey              string `json:"api_key"`
	Encoded             string `json:"encoded"`
	BeatsLogstashFormat string `json:"beats_logstash_format"`
}

type metricsClient struct {
	client *elasticsearch.TypedClient
}

func NewMetricsReader(backendAddr string) (core.MetricsReader, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			backendAddr,
		},
		//APIKey: apiKeyConfig.ApiKey,
	}
	es, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		return nil, err
	}

	return &metricsClient{
		client: es,
	}, nil
}

func (c *metricsClient) EnsureIndex(ctx context.Context, indexName string) error {
	// Check if index exists
	exists, err := c.client.Indices.Exists(indexName).Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to check index existence: %w", err)
	}
	if exists {
		log.Printf("Index %s already exists", indexName)
		return nil
	}

	// Create index with mapping
	mapping := types.TypeMapping{
		Properties: map[string]types.Property{
			"rule":       types.NewKeywordProperty(),
			"status":     types.NewKeywordProperty(),
			"service":    types.NewKeywordProperty(),
			"latency":    types.NewFloatNumberProperty(),
			"@timestamp": types.NewDateProperty(),
		},
	}

	req := create.Request{
		Mappings: &mapping,
	}

	_, err = c.client.Indices.Create(indexName).Request(&req).Do(ctx)
	if err != nil {
		return err
	}

	log.Printf("Index %s created successfully", indexName)
	return nil
}

func (c metricsClient) QueryNodeMetrics() ([]core.NodeMetrics, error) {
	size := 1
	nameField := "kubernetes.node.name"
	cpuUtilField := "k8s.node.cpu.utilization"
	nodesAggName := "nodes"
	metricsAggName := "latest_metrics"
	res, err := c.client.Search().
		Size(0).
		Index(metricsIndex).
		Request(&search.Request{
			Query: &types.Query{
				Bool: &types.BoolQuery{
					Must: []types.Query{
						{
							Exists: &types.ExistsQuery{
								Field: cpuUtilField,
							},
						},
						{
							Exists: &types.ExistsQuery{
								Field: nameField,
							},
						},
					},
				},
			},
			Aggregations: map[string]types.Aggregations{
				nodesAggName: {
					Terms: &types.TermsAggregation{
						Field: &nameField,
					},
					Aggregations: map[string]types.Aggregations{
						metricsAggName: {
							TopHits: &types.TopHitsAggregation{
								Sort: []types.SortCombinations{
									types.SortOptions{
										SortOptions: map[string]types.FieldSort{
											"@timestamp": {
												Order: &sortorder.Desc,
											},
										},
									},
								},
								Size: &size,
							},
						},
					},
				},
			},
		}).
		Do(context.Background())

	if err != nil {
		return nil, err
	}

	aggregation := res.Aggregations[nodesAggName]
	nodesAgg, ok := aggregation.(*types.StringTermsAggregate)
	if !ok {
		return nil, errors.New("incorrect aggregation type")
	}

	buckets, ok := nodesAgg.Buckets.([]types.StringTermsBucket)
	if !ok {
		return nil, errors.New("incorrect bucket type")
	}
	ret := []core.NodeMetrics{}
	for _, bucket := range buckets {
		aggregation = bucket.Aggregations[metricsAggName]
		bucketAgg := aggregation.(*types.TopHitsAggregate)
		source := bucketAgg.Hits.Hits[0].Source_

		nodeMetrics, err := unmarshalSource(source)
		if err != nil {
			return nil, err
		}
		nodeMetrics.Node = core.Node(bucket.Key.(string))
		ret = append(ret, nodeMetrics)
	}

	return ret, nil
}

type metricExtractorFunc func(agg types.Aggregate) (float64, bool)

func (c metricsClient) Query95thPercentileAppRuntimes(timeRangeGte string) (map[string]float64, map[string]int, error) {

	const metricAggName = "p95_trace_duration"
	metricAgg := types.Aggregations{
		PercentilesBucket: &types.PercentilesBucketAggregation{
			// The field here refers to the output of the "trace_duration_ms" bucket script
			BucketsPath: strPtr("traces>trace_duration_ms"),
			Percents:    []types.Float64{95.0},
		},
	}

	extractorFunc := func(agg types.Aggregate) (float64, bool) {
		p95Interface, ok := agg.(*types.PercentilesBucketAggregate)
		if ok {
			if values, ok := p95Interface.Values.(map[string]interface{}); ok {
				if p95, found := values["95.0"]; found {
					if f, ok := p95.(float64); ok {
						return f, true
					}
				}
			}
		}
		return 0, false
	}

	return c.queryAppRuntimes(timeRangeGte, metricAggName, metricAgg, extractorFunc)

}

func (c metricsClient) QueryAverageAppRuntimes(timeRangeGte string) (map[string]float64, map[string]int, error) {
	const metricAggName = "avg_trace_duration"

	metricAgg := types.Aggregations{
		AvgBucket: &types.AverageBucketAggregation{
			BucketsPath: strPtr("traces>trace_duration_ms"),
		},
	}

	extractorFunc := func(agg types.Aggregate) (float64, bool) {
		if avgAgg, ok := agg.(*types.SimpleValueAggregate); ok {
			if avgAgg.Value != nil {
				return float64(*avgAgg.Value), true
			}

		}
		return 0, false
	}

	return c.queryAppRuntimes(timeRangeGte, metricAggName, metricAgg, extractorFunc)
}

// QueryComponentRuntimes returns, per app and span name, the average span duration (ms) and the span count
// in the time range: the measured processing time of each component under the current load
func (c metricsClient) QueryComponentRuntimes(timeRangeGte string) (map[string]map[string]core.ComponentRuntime, error) {
	size := 1000
	res, err := c.client.Search().
		Index(tracesIndex).
		Request(&search.Request{
			Size: intPtr(0),
			Query: &types.Query{
				Bool: &types.BoolQuery{
					Filter: []types.Query{
						{
							Range: map[string]types.RangeQuery{
								"@timestamp": &types.DateRangeQuery{
									Gte: strPtr(timeRangeGte),
									Lte: strPtr("now"),
								},
							},
						},
						{
							Term: map[string]types.TermQuery{
								"processor.event": {
									Value: "span",
								},
							},
						},
					},
				},
			},
			Aggregations: map[string]types.Aggregations{
				"apps": {
					Terms: &types.TermsAggregation{Field: strPtr("labels.app_name"), Size: &size},
					Aggregations: map[string]types.Aggregations{
						"spans": {
							Terms: &types.TermsAggregation{Field: strPtr("span.name"), Size: &size},
							Aggregations: map[string]types.Aggregations{
								"avg_duration_us": {
									Avg: &types.AverageAggregation{Field: strPtr("span.duration.us")},
								},
							},
						},
					},
				},
			},
		}).
		Do(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error querying component runtimes: %w", err)
	}

	result := make(map[string]map[string]core.ComponentRuntime)
	appsAgg, ok := res.Aggregations["apps"].(*types.StringTermsAggregate)
	if !ok {
		return result, nil // no spans in the time range
	}
	for _, appBucket := range appsAgg.Buckets.([]types.StringTermsBucket) {
		spansAgg, ok := appBucket.Aggregations["spans"].(*types.StringTermsAggregate)
		if !ok {
			continue
		}
		runtimes := make(map[string]core.ComponentRuntime)
		for _, spanBucket := range spansAgg.Buckets.([]types.StringTermsBucket) {
			avg, ok := spanBucket.Aggregations["avg_duration_us"].(*types.AvgAggregate)
			if !ok || avg.Value == nil {
				continue
			}
			runtimes[spanBucket.Key.(string)] = core.ComponentRuntime{
				AvgMs: float64(*avg.Value) / 1000,
				Count: int(spanBucket.DocCount),
			}
		}
		result[appBucket.Key.(string)] = runtimes
	}
	return result, nil
}

func (c metricsClient) queryAppRuntimes(
	timeRangeGte string,
	metricAggName string,
	metricAgg types.Aggregations,
	extractor metricExtractorFunc,
) (map[string]float64, map[string]int, error) {

	size := 1000
	appNameField := "labels.app_name"
	startSpanLabelField := "labels.trace_boundary_start"
	endSpanLabelField := "labels.trace_boundary_end"

	appAggregations := map[string]types.Aggregations{
		"trace_count": {
			Cardinality: &types.CardinalityAggregation{
				Field: strPtr("trace.id"),
			},
		},
		"traces": {
			Terms: &types.TermsAggregation{
				Field: strPtr("trace.id"),
				Size:  &size,
			},
			Aggregations: map[string]types.Aggregations{
				"has_start_span": {
					Filter: &types.Query{
						Exists: &types.ExistsQuery{
							Field: startSpanLabelField,
						},
					},
				},
				"has_end_span": {
					Filter: &types.Query{
						Exists: &types.ExistsQuery{
							Field: endSpanLabelField,
						},
					},
				},
				"completeness_filter": {
					BucketSelector: &types.BucketSelectorAggregation{
						BucketsPath: map[string]string{
							"start_count": "has_start_span._count",
							"end_count":   "has_end_span._count",
						},
						Script: &types.Script{
							// Only include traces that have both a start span AND at least one end span.
							Source: strPtr("params.start_count >= 1 && params.end_count >= 1"),
						},
					},
				},
				"min_start": {
					Min: &types.MinAggregation{
						Field: strPtr("@timestamp"),
					},
				},
				"max_end": {
					Max: &types.MaxAggregation{
						Script: &types.Script{
							Source: strPtr("doc['@timestamp'].value.toInstant().toEpochMilli() + doc['span.duration.us'].value / 1000"),
						},
					},
				},
				"trace_duration_ms": {
					BucketScript: &types.BucketScriptAggregation{
						BucketsPath: map[string]string{
							"min_start": "min_start.value",
							"max_end":   "max_end.value",
						},
						Script: &types.Script{
							Source: strPtr("params.max_end - params.min_start"),
						},
					},
				},
			},
		},
	}

	appAggregations[metricAggName] = metricAgg

	res, err := c.client.Search().
		Index(tracesIndex).
		Request(&search.Request{
			Size: intPtr(0),
			Query: &types.Query{
				Bool: &types.BoolQuery{
					Filter: []types.Query{
						{
							Range: map[string]types.RangeQuery{
								"@timestamp": &types.DateRangeQuery{
									Gte: strPtr(timeRangeGte),
									Lte: strPtr("now"),
								},
							},
						},
						{
							Term: map[string]types.TermQuery{
								"processor.event": {
									Value: "span",
								},
							},
						},
					},
				},
			},
			Aggregations: map[string]types.Aggregations{
				"apps": {
					Terms: &types.TermsAggregation{
						Field: &appNameField,
						Size:  &size,
					},
					Aggregations: appAggregations,
				},
			},
		}).
		Do(context.Background())

	if err != nil {
		return nil, nil, fmt.Errorf("error querying metrics: %w", err)
	}

	appsInterface, exists := res.Aggregations["apps"]
	if !exists || appsInterface == nil {
		return make(map[string]float64), make(map[string]int), nil
	}

	appsAgg, ok := appsInterface.(*types.StringTermsAggregate)
	if !ok {
		return nil, nil, errors.New("incorrect aggregation type for apps")
	}

	metricResult := make(map[string]float64)
	countResult := make(map[string]int)

	for _, appBucket := range appsAgg.Buckets.([]types.StringTermsBucket) {
		appName := appBucket.Key.(string)

		// Extract Metric (P95, Avg, etc.) using the injected extractor
		metricInterface, ok := appBucket.Aggregations[metricAggName]
		if ok && metricInterface != nil {
			if value, ok := extractor(metricInterface); ok {
				metricResult[appName] = value
			}
		}

		countInterface, ok := appBucket.Aggregations["trace_count"]
		if ok && countInterface != nil {
			if countAgg, ok := countInterface.(*types.CardinalityAggregate); ok {
				countResult[appName] = int(countAgg.Value)
			}
		}
	}

	return metricResult, countResult, nil
}

func strPtr(s string) *string { return &s }
func intPtr(v int) *int       { return &v }

func unmarshalSource(source json.RawMessage) (core.NodeMetrics, error) {
	var sourceMap map[string]json.RawMessage
	err := json.Unmarshal(source, &sourceMap)
	if err != nil {
		return core.NodeMetrics{}, err
	}

	var k8s map[string]json.RawMessage
	err = json.Unmarshal(sourceMap["k8s"], &k8s)
	if err != nil {
		return core.NodeMetrics{}, err
	}

	var nodeMetrics core.NodeMetrics
	err = json.Unmarshal(k8s["node"], &nodeMetrics)
	if err != nil {
		return nodeMetrics, err
	}

	return nodeMetrics, nil
}
