package routing

import (
	"encoding/json"
	"fmt"
	"log"
	"lsf-configurator/pkg/core"

	redis "github.com/go-redis/redis/v7"
)

type RouteConfigurator struct {
	RedisUrl string
	RedisCli redis.Client
}

const LocalRoute = "local"

func NewRouteConfigurator(redisUrl string) core.RoutingClient {
	return &RouteConfigurator{
		RedisUrl: redisUrl,
		RedisCli: *redis.NewClient(&redis.Options{Addr: redisUrl}),
	}
}

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
