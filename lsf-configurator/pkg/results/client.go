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

func NewRedisResultsClient(redisUrl string) core.ResultsClient {
	return &redisResultsClient{
		RedisUrl: redisUrl,
		RedisCli: *redis.NewClient(&redis.Options{Addr: redisUrl}),
	}
}

func (rc *redisResultsClient) GetAppResults(appId string, count int) ([]core.AppResult, error) {
	res, err := rc.RedisCli.LRange(resultsKeyPrefix+appId, 0, int64(count-1)).Result()
	if err != nil {
		return nil, err
	}

	results := make([]core.AppResult, 0, len(res))

	for _, r := range res {
		var ar core.AppResult
		if err := json.Unmarshal([]byte(r), &ar); err != nil {
			return nil, fmt.Errorf("failed to unmarshal redis result for app %s: %w", appId, err)
		}
		results = append(results, ar)
	}

	return results, nil
}
