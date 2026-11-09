package routing

// RoutingTable is the routing table of a deployment as stored in Redis: the routes of each component.
type RoutingTable map[string][]Route

// Route is a stored route: the next component and the URL of the deployment that runs it, or LocalRoute.
type Route struct {
	Component string `json:"component"`
	Url       string `json:"url"`
}
