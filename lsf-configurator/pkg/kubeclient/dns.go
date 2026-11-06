package kubeclient

import (
	"context"
	"fmt"
	"log"
	"reflect"

	networkingv1beta1 "istio.io/api/networking/v1beta1"
	istionetworking "istio.io/client-go/pkg/apis/networking/v1beta1"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const knativeLocalGateway = "knative-local-gateway.istio-system.svc.cluster.local"

func (c *Client) EnsureDNSRecord(ctx context.Context, namespace, appName, targetServiceName string) error {
	vsName := generateServiceName(appName)
	targetHost := fmt.Sprintf("%s.%s.svc.cluster.local", targetServiceName, namespace)
	hostDomain := fmt.Sprintf("%s.%s.127.0.0.1.sslip.io", vsName, namespace)

	vsClient := c.istio.NetworkingV1beta1().VirtualServices(namespace)

	desiredSpec := networkingv1beta1.VirtualService{
		Hosts:    []string{hostDomain, fmt.Sprintf("%s.%s.svc", targetServiceName, namespace), targetHost},
		Gateways: []string{"knative-serving/knative-ingress-gateway"},
		Http: []*networkingv1beta1.HTTPRoute{
			{
				Route: []*networkingv1beta1.HTTPRouteDestination{
					{
						Destination: &networkingv1beta1.Destination{
							Host: knativeLocalGateway,
							Port: &networkingv1beta1.PortSelector{Number: 80},
						},
					},
				},
				Rewrite: &networkingv1beta1.HTTPRewrite{
					Authority: targetHost,
				},
			},
		},
	}

	existing, err := vsClient.Get(ctx, vsName, metav1.GetOptions{})

	if errors.IsNotFound(err) {
		newVS := &istionetworking.VirtualService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vsName,
				Namespace: namespace,
				Labels: map[string]string{
					"app":  appName,
					"type": "faas-dns",
				},
			},
			Spec: networkingv1beta1.VirtualService{
				Hosts:    desiredSpec.Hosts,
				Gateways: desiredSpec.Gateways,
				Http:     desiredSpec.Http,
			},
		}

		if _, createErr := vsClient.Create(ctx, newVS, metav1.CreateOptions{}); createErr != nil {
			return fmt.Errorf("failed to create VirtualService: %w", createErr)
		}
		log.Printf("Created VirtualService %q → %q\n", hostDomain, targetHost)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get VirtualService: %w", err)
	}

	// Update existing fields only if changed
	updated := false
	if !reflect.DeepEqual(existing.Spec.Hosts, desiredSpec.Hosts) {
		existing.Spec.Hosts = desiredSpec.Hosts
		updated = true
	}
	if !reflect.DeepEqual(existing.Spec.Gateways, desiredSpec.Gateways) {
		existing.Spec.Gateways = desiredSpec.Gateways
		updated = true
	}
	if !reflect.DeepEqual(existing.Spec.Http, desiredSpec.Http) {
		existing.Spec.Http = desiredSpec.Http
		updated = true
	}
	if updated {
		if _, updateErr := vsClient.Update(ctx, existing, metav1.UpdateOptions{}); updateErr != nil {
			return fmt.Errorf("failed to update VirtualService: %w", updateErr)
		}
		log.Printf("Updated VirtualService %q → %q\n", hostDomain, targetHost)
	}

	return nil
}

// DeleteDNSRecord deletes the VirtualService associated with an app.
func (c *Client) DeleteDNSRecord(ctx context.Context, namespace, appName string) error {
	vsName := generateServiceName(appName)
	err := c.istio.NetworkingV1beta1().VirtualServices(namespace).Delete(ctx, vsName, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete VirtualService %q: %w", vsName, err)
	}
	log.Printf("Deleted VirtualService %q\n", vsName)
	return nil
}

func generateServiceName(appName string) string {
	return fmt.Sprintf("app-%s", appName)
}
