// Package results reads the final results of function apps from Redis.
package results

import (
	"encoding/json"
	"fmt"
	"lsf-configurator/pkg/core"

	"github.com/go-redis/redis/v7"
)

type redisResultsClient struct {
	RedisUrl string
	RedisCli redis.Client
}

const resultsKeyPrefix = "result:"

// NewRedisResultsClient returns a ResultsClient that reads the result:<app id> lists from the Redis at redisURL.
func NewRedisResultsClient(redisURL string) core.ResultsClient {
	return &redisResultsClient{
		RedisUrl: redisURL,
		RedisCli: *redis.NewClient(&redis.Options{Addr: redisURL}),
	}
}

func (rc *redisResultsClient) GetAppResults(appID string, count int) ([]core.AppResult, error) {
	res, err := rc.RedisCli.LRange(resultsKeyPrefix+appID, 0, int64(count-1)).Result()
	if err != nil {
		return nil, err
	}

	results := make([]core.AppResult, 0, len(res))

	for _, r := range res {
		var ar core.AppResult
		if err := json.Unmarshal([]byte(r), &ar); err != nil {
			return nil, fmt.Errorf("failed to unmarshal redis result for app %s: %w", appID, err)
		}
		results = append(results, ar)
	}

	return results, nil
}
