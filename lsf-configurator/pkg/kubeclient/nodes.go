package kubeclient

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// OverloadedAnnotation marks a node as overloaded ("true"). It stands in for an overload detection (e.g. from the
// node metrics): an operator or an experiment sets it, and the controller moves the layouts off the node.
const OverloadedAnnotation = "lsf-configurator/overloaded"

var nodeResource = schema.GroupVersionResource{Version: "v1", Resource: "nodes"}

// NodeAvailability returns every node of the cluster and whether new deployments can be placed on it: it is
// ready, schedulable (not cordoned) and not marked overloaded.
func (c *Client) NodeAvailability(ctx context.Context) (map[string]bool, error) {
	nodes, err := c.dynamic.Resource(nodeResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}
	availability := make(map[string]bool, len(nodes.Items))
	for i := range nodes.Items {
		node := &nodes.Items[i]
		unschedulable, _, _ := unstructured.NestedBool(node.Object, "spec", "unschedulable")
		availability[node.GetName()] = conditionStatus(node, "Ready") == "True" && !unschedulable &&
			node.GetAnnotations()[OverloadedAnnotation] != "true"
	}
	return availability, nil
}
