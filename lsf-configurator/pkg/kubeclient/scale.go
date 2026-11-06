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

// latestRevision returns the latest ready revision of a Knative service; its PodAutoscaler has the same name
func (c *Client) latestRevision(ctx context.Context, namespace, serviceName string) (string, error) {
	ksvc, err := c.dynamic.Resource(ksvcResource).Namespace(namespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get knative service %s/%s: %w", namespace, serviceName, err)
	}
	revision, found, err := unstructured.NestedString(ksvc.Object, "status", "latestReadyRevisionName")
	if err != nil || !found || revision == "" {
		return "", fmt.Errorf("no ready revision for knative service %s/%s", namespace, serviceName)
	}
	return revision, nil
}

// SetMinScale changes the min-scale of a Knative service on the PodAutoscaler of its latest ready revision.
// Changing the annotation on the service template would roll out a new revision, and Knative does not enforce
// min-scale on a revision until it becomes routable, so the new pods could be scaled away right before a switch.
func (c *Client) SetMinScale(ctx context.Context, namespace, serviceName string, minScale int) error {
	revision, err := c.latestRevision(ctx, namespace, serviceName)
	if err != nil {
		return err
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

	_, err = c.dynamic.Resource(paResource).Namespace(namespace).Patch(ctx, revision, types.MergePatchType, body, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to set min-scale of %s/%s to %d: %w", namespace, revision, minScale, err)
	}
	return nil
}

// ReadyPods returns how many pods of a Knative service are ready to serve: the actual scale
// that Knative reports on the PodAutoscaler of the latest ready revision
func (c *Client) ReadyPods(ctx context.Context, namespace, serviceName string) (int, error) {
	revision, err := c.latestRevision(ctx, namespace, serviceName)
	if err != nil {
		return 0, err
	}
	pa, err := c.dynamic.Resource(paResource).Namespace(namespace).Get(ctx, revision, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to get podautoscaler %s/%s: %w", namespace, revision, err)
	}
	ready, found, err := unstructured.NestedInt64(pa.Object, "status", "actualScale")
	if err != nil || !found {
		return 0, nil // not reported yet: no ready pods
	}
	return int(ready), nil
}
