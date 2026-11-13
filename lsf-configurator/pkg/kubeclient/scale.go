package kubeclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

var (
	ksvcResource     = schema.GroupVersionResource{Group: "serving.knative.dev", Version: "v1", Resource: "services"}
	paResource       = schema.GroupVersionResource{Group: "autoscaling.internal.knative.dev", Version: "v1alpha1", Resource: "podautoscalers"}
	revisionResource = schema.GroupVersionResource{Group: "serving.knative.dev", Version: "v1", Resource: "revisions"}
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

// restartAnnotation is set on the revision template of a Knative service to roll out a new revision
const restartAnnotation = "lsf-configurator/restarted-at"

// RepairRevision replaces a broken revision of a Knative service with a new one and reports whether it did. A
// revision is broken when it was ready (it is the latest ready revision) but its container is no longer healthy:
// Knative then keeps it as the latest ready revision, does not repair it and no longer holds its min-scale. A
// revision that never became ready (e.g. a bad image) is left alone, so a failing rollout is not repeated.
func (c *Client) RepairRevision(ctx context.Context, namespace, serviceName string) (bool, error) {
	services := c.dynamic.Resource(ksvcResource).Namespace(namespace)
	ksvc, err := services.Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to get knative service %s/%s: %w", namespace, serviceName, err)
	}
	created, _, _ := unstructured.NestedString(ksvc.Object, "status", "latestCreatedRevisionName")
	ready, _, _ := unstructured.NestedString(ksvc.Object, "status", "latestReadyRevisionName")
	if created == "" || created != ready {
		return false, nil // a new revision is rolling out, or none was ever ready
	}
	revision, err := c.dynamic.Resource(revisionResource).Namespace(namespace).Get(ctx, created, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to get revision %s/%s: %w", namespace, created, err)
	}
	if conditionStatus(revision, "ContainerHealthy") != "False" {
		return false, nil
	}

	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"annotations": map[string]string{restartAnnotation: time.Now().UTC().Format(time.RFC3339)},
				},
			},
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return false, fmt.Errorf("failed to marshal the revision restart patch: %w", err)
	}
	if _, err := services.Patch(ctx, serviceName, types.MergePatchType, body, metav1.PatchOptions{}); err != nil {
		return false, fmt.Errorf("failed to roll out a new revision of %s/%s: %w", namespace, serviceName, err)
	}
	return true, nil
}

// conditionStatus returns the status ("True", "False", "Unknown") of a condition of a Knative resource, or ""
// when the resource does not report it
func conditionStatus(obj *unstructured.Unstructured, conditionType string) string {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, c := range conditions {
		condition, ok := c.(map[string]interface{})
		if ok && condition["type"] == conditionType {
			status, _ := condition["status"].(string)
			return status
		}
	}
	return ""
}
