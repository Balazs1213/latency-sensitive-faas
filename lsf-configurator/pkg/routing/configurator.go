// Package routing stores the routing tables of deployments in Redis, where the function compositions read them.
package routing

import (
	"encoding/json"
	"fmt"
	"log"
	"lsf-configurator/pkg/core"

	redis "github.com/go-redis/redis/v7"
)

// RouteConfigurator stores the routing tables of deployments in Redis under the deployment ID.
type RouteConfigurator struct {
	RedisUrl string
	RedisCli redis.Client
}

// LocalRoute is the route target of a component that runs in the same composition.
const LocalRoute = "local"

// NewRouteConfigurator returns a RoutingClient that writes to the Redis at redisUrl.
func NewRouteConfigurator(redisUrl string) core.RoutingClient {
	return &RouteConfigurator{
		RedisUrl: redisUrl,
		RedisCli: *redis.NewClient(&redis.Options{Addr: redisUrl}),
	}
}

// SetRoutingTable converts the routing table of a deployment, replacing deployment IDs with their cluster URLs,
// and stores it in Redis under the deployment ID.
func (rc *RouteConfigurator) SetRoutingTable(deployment core.Deployment) error {
	rtDto := make(RoutingTable)

	for component, routes := range deployment.RoutingTable {
		for _, r := range routes {
			var url string
			if r.Function == LocalRoute {
				url = LocalRoute
			} else {
				url = getFunctionUrl(r.Function, deployment.Namespace)
			}

			routeDto := Route{
				Component: r.To,
				Url:       url,
			}
			rtDto[string(component)] = append(rtDto[string(component)], routeDto)
		}
	}

	data, err := json.Marshal(rtDto)
	if err != nil {
		return err
	}

	err = rc.RedisCli.Set(deployment.Id, data, 0).Err()
	if err != nil {
		return err
	}

	log.Printf("Routing table for %s set successfully, rt: %s", deployment.Id, data)
	return nil
}

// DeleteRoutingTable removes the routing table of a deployment from Redis.
func (rc *RouteConfigurator) DeleteRoutingTable(deploymentId string) error {
	err := rc.RedisCli.Del(deploymentId).Err()
	if err != nil {
		return err
	}

	log.Printf("Routing table for %s deleted successfully", deploymentId)
	return nil
}

func getFunctionUrl(fcId, namespace string) string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local", fcId, namespace)
}
