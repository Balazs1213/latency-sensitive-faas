package kubeclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

var (
	ksvcResource = schema.GroupVersionResource{Group: "serving.knative.dev", Version: "v1", Resource: "services"}
	paResource   = schema.GroupVersionResource{Group: "autoscaling.internal.knative.dev", Version: "v1alpha1", Resource: "podautoscalers"}
)

// SetMinScale changes the min-scale of a Knative service on the PodAutoscaler of its latest ready revision.
// Changing the annotation on the service template would roll out a new revision, and Knative does not enforce
// min-scale on a revision until it becomes routable, so the new pods could be scaled away right before a switch.
func (c *Client) SetMinScale(ctx context.Context, namespace, serviceName string, minScale int) error {
	ksvc, err := c.dynamic.Resource(ksvcResource).Namespace(namespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get knative service %s/%s: %w", namespace, serviceName, err)
	}
	revision, found, err := unstructured.NestedString(ksvc.Object, "status", "latestReadyRevisionName")
	if err != nil || !found || revision == "" {
		return fmt.Errorf("no ready revision for knative service %s/%s", namespace, serviceName)
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				"autoscaling.knative.dev/min-scale": strconv.Itoa(minScale),
			},
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal min-scale patch: %w", err)
	}

	// The PodAutoscaler has the same name as the revision
	_, err = c.dynamic.Resource(paResource).Namespace(namespace).Patch(ctx, revision, types.MergePatchType, body, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to set min-scale of %s/%s to %d: %w", namespace, revision, minScale, err)
	}
	return nil
}