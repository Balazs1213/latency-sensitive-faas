// Package builder builds the container images of function compositions with Tekton pipelines.
package builder

import (
	"context"
	"fmt"
	"log"
	"lsf-configurator/pkg/core"
	"lsf-configurator/pkg/uuid"
	"os"
	"os/user"
	"path/filepath"
	"time"

	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	tektonclientset "github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// TimeoutDuration is how long a build holds its slot of the concurrency limit if no build notification frees it
// earlier.
const TimeoutDuration = 8 * time.Minute // Default timeout for builds

// TektonConfig holds the Tekton settings of the image builds: the namespace and pipeline to run, the notification
// URL, the workspace PVC, the image registry and repository, and the service account.
type TektonConfig struct {
	Namespace      string
	Pipeline       string
	NotifyURL      string
	WorkspacePVC   string
	ImageRegistry  string
	ImageRepo      string
	ServiceAccount string
}

// TektonBuilder starts Tekton PipelineRuns that build the images of function compositions, with a limit on how
// many builds run at the same time.
type TektonBuilder struct {
	TektonConfig
	concurrencyLimiter chan struct{}
}

// NewTektonBuilder creates a TektonBuilder that runs at most concurrencyLimit builds at the same time.
func NewTektonBuilder(cfg TektonConfig, concurrencyLimit int) *TektonBuilder {
	return &TektonBuilder{
		TektonConfig:       cfg,
		concurrencyLimiter: make(chan struct{}, concurrencyLimit),
	}
}

// Build starts a PipelineRun that builds the image of fc from buildDir, without waiting for it to finish. It blocks
// while the concurrency limit is reached; a slot is freed by NotifyBuildFinished or after TimeoutDuration.
func (b *TektonBuilder) Build(ctx context.Context, fc core.FunctionComposition, buildDir string) error {
	// Acquire semaphore
	b.concurrencyLimiter <- struct{}{}

	// Start a timeout goroutine to release the semaphore after a certain duration
	go func() {
		select {
		case <-time.After(TimeoutDuration):
			b.releaseSemaphore()
		case <-ctx.Done():

		}
	}()

	var config *rest.Config
	var err error

	config, err = rest.InClusterConfig()
	if err != nil {
		// Fallback to local kubeconfig if not running in cluster
		usr, _ := user.Current()
		kubeconfig := filepath.Join(usr.HomeDir, ".kube", "config")
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Printf("Error getting kube config: %v", err)
			return fmt.Errorf("failed to get kube config: %w", err)
		}
	}

	tektonClient, err := tektonclientset.NewForConfig(config)
	if err != nil {
		log.Printf("Error creating Tekton client: %v", err)
		return fmt.Errorf("failed to create tekton client: %w", err)
	}

	image := fmt.Sprintf("%s/%s/%s", b.ImageRegistry, b.ImageRepo, fc.Id)
	prName := fmt.Sprintf("build-%s", uuid.New())

	uploadsFolder := getEnv("UPLOAD_DIR", "/uploads")
	relContextDir, err := filepath.Rel(uploadsFolder, buildDir)
	if err != nil {
		log.Printf("Error computing relative path for CONTEXT_DIR: %v", err)
		return fmt.Errorf("failed to compute relative CONTEXT_DIR: %w", err)
	}

	pr := &tektonv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prName,
			Namespace: b.Namespace,
		},
		Spec: tektonv1.PipelineRunSpec{
			PipelineRef: &tektonv1.PipelineRef{
				Name: b.Pipeline,
			},
			Params: []tektonv1.Param{
				{Name: "IMAGE", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: image}},
				{Name: "CONTEXT_DIR", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: relContextDir}},
				{Name: "NOTIFY_URL", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: b.NotifyURL}},
				{Name: "FC_ID", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: fc.Id}},
			},
			Workspaces: []tektonv1.WorkspaceBinding{
				{
					Name: "source",
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: b.WorkspacePVC,
					},
				},
			},
			TaskRunTemplate: tektonv1.PipelineTaskRunTemplate{
				ServiceAccountName: b.ServiceAccount,
			},
		},
	}

	_, err = tektonClient.TektonV1().PipelineRuns(b.Namespace).Create(ctx, pr, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error creating PipelineRun: %v", err)
		b.releaseSemaphore() // Ensure semaphore is released on error
		return fmt.Errorf("failed to create PipelineRun: %w", err)
	}
	log.Printf("PipelineRun %s created successfully in namespace %s", prName, b.Namespace)
	return nil
}

// NotifyBuildFinished frees the concurrency slot of a finished build.
func (b *TektonBuilder) NotifyBuildFinished() {
	b.releaseSemaphore()
}

func (b *TektonBuilder) releaseSemaphore() {
	select {
	case <-b.concurrencyLimiter:
		// Semaphore released successfully
	default:
		// Semaphore already released, no action needed
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
