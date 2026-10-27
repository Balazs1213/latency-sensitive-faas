package kubeclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

var ksvcResource = schema.GroupVersionResource{Group: "serving.knative.dev", Version: "v1", Resource: "services"}

// SetMinScale changes the min-scale annotation of a Knative service.
// Knative rolls out a new revision; the old revision keeps serving until the new one is ready.
func (c *Client) SetMinScale(ctx context.Context, namespace, serviceName string, minScale int) error {
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"annotations": map[string]string{
						"autoscaling.knative.dev/min-scale": strconv.Itoa(minScale),
					},
				},
			},
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal min-scale patch: %w", err)
	}

	_, err = c.dynamic.Resource(ksvcResource).Namespace(namespace).Patch(ctx, serviceName, types.MergePatchType, body, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to set min-scale of %s/%s to %d: %w", namespace, serviceName, minScale, err)
	}
	return nil
}