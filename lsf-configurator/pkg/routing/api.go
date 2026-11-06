package routing

type RoutingTable map[string][]Route

type Route struct {
	Component string `json:"component"`
	Url       string `json:"url"`
}
