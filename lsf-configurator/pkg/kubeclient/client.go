// Package kubeclient manages Kubernetes resources of the function apps: the Istio VirtualServices of their entry
// points and the scale of their Knative services.
package kubeclient

import (
	"fmt"
	"os"
	"path/filepath"

	istioclient "istio.io/client-go/pkg/clientset/versioned"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Client accesses the cluster through an Istio client and a dynamic client.
type Client struct {
	istio   istioclient.Interface
	dynamic dynamic.Interface // used to patch Knative services (no typed Knative client in the module)
}

// NewKubeclient creates a Client from the in-cluster configuration, or from the local kubeconfig outside a cluster.
func NewKubeclient() (*Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		// Try local kubeconfig for local debugging
		home, homeErr := os.UserHomeDir()
		if homeErr != nil {
			return nil, fmt.Errorf("failed to load in-cluster config: %v, and failed to get home dir: %w", err, homeErr)
		}
		kubeconfig := filepath.Join(home, ".kube", "config")
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to load in-cluster config: %v, and failed to load local kubeconfig: %w", err, err)
		}
	}

	ic, err := istioclient.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Istio client: %w", err)
	}

	dc, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return &Client{istio: ic, dynamic: dc}, nil
}
