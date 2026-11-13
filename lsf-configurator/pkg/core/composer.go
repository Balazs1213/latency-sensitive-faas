// Package core holds the domain models and interfaces of the platform, the Composer that builds and deploys
// function compositions, and the latency controller that switches the apps between layouts. The implementations of
// the interfaces live in sibling packages.
package core

import (
	"context"
	"fmt"
	"lsf-configurator/pkg/filesystem"
	"lsf-configurator/pkg/uuid"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apex/log"
)

// Settings of the Composer's worker pool: how many times a failed task is retried, the number of workers and the
// size of the task queue.
const (
	MaxRetries     = 3
	WorkerPoolSize = 10
	QueueSize      = 30
)

// for now, only python is implemented
var runtimeExtensions = map[string]string{
	"python": ".py", // Python
}

// Composer orchestrates function apps, function compositions and deployments: it persists them and runs their
// builds and deployments asynchronously on a worker pool.
type Composer struct {
	knClient           KnClient
	scheduler          Scheduler
	routingClient      RoutingClient
	builder            Builder
	dnsClient          DNSClient
	scaleClient        ScaleClient
	functionAppRepo    FunctionAppRepository
	fcRepo             FunctionCompositionRepository
	deploymentRepo     DeploymentRepository
	metricsReader      MetricsReader
	pendingDeployments map[string]chan Result // key = deploymentId
	mu                 sync.Mutex
}

// NewComposer creates a Composer with its own worker pool.
func NewComposer(
	functionAppRepo FunctionAppRepository,
	fcRepo FunctionCompositionRepository,
	deploymentRepo DeploymentRepository,
	routingClient RoutingClient,
	knClient KnClient,
	builder Builder,
	metricsReader MetricsReader,
	dnsClient DNSClient,
	scaleClient ScaleClient,
) *Composer {
	scheduler := NewScheduler(WorkerPoolSize, QueueSize)
	return &Composer{
		knClient:        knClient,
		scheduler:       scheduler,
		routingClient:   routingClient,
		builder:         builder,
		dnsClient:       dnsClient,
		scaleClient:     scaleClient,
		functionAppRepo: functionAppRepo,
		fcRepo:          fcRepo,
		deploymentRepo:  deploymentRepo,
		metricsReader:   metricsReader,
	}
}

// --- FUNCTION APPS ---

// GetFunctionApp returns the function app with the given ID.
func (c *Composer) GetFunctionApp(appID string) (*FunctionApp, error) {
	return c.functionAppRepo.GetByID(appID)
}

// ListFunctionApps returns all function apps.
func (c *Composer) ListFunctionApps() ([]*FunctionApp, error) {
	return c.functionAppRepo.GetAll()
}

// CreateFunctionApp creates and persists a function app and saves its uploaded files into a new directory under
// the upload directory. Uploaded files that belong to no component are recorded as the app's shared files.
func (c *Composer) CreateFunctionApp(creationData FunctionAppCreationData) (*FunctionApp, error) {
	id := uuid.New()
	fcApp := FunctionApp{
		Id:           id,
		Name:         creationData.AppName,
		Compositions: make([]*FunctionComposition, 0),
		Files:        make([]string, 0),
		SourcePath:   "",
		Components:   creationData.Components,
		Links:        creationData.Links,
		Runtime:      strings.ToLower(creationData.Runtime),
		LatencyLimit: creationData.LatencyLimit,
	}

	appDir := filepath.Join(creationData.UploadDir, fcApp.Id)
	err := filesystem.CreateDir(appDir)
	if err != nil {
		return nil, fmt.Errorf("could not create directory for app files: %s", err.Error())
	}
	fcApp.SourcePath = appDir

	for _, fileHeader := range creationData.Files {
		fileName := fileHeader.Filename
		// Source files that don't belong to a declared component (e.g. shared
		// modules imported by several components) are kept as regular files.
		if !isComponent(fileName, fcApp.Runtime, creationData.Components) {
			fcApp.Files = append(fcApp.Files, fileName)
		}
		err := filesystem.SaveMultiPartFile(fileHeader, appDir)
		if err != nil {
			return nil, err
		}
	}

	if err := c.functionAppRepo.Save(&fcApp); err != nil {
		return nil, fmt.Errorf("could not persist function app: %w", err)
	}

	return &fcApp, nil
}

// UpdateFunctionApp persists the changes of a function app.
func (c *Composer) UpdateFunctionApp(app *FunctionApp) error {
	if app == nil {
		return fmt.Errorf("function app is nil")
	}
	if err := c.functionAppRepo.Save(app); err != nil {
		return fmt.Errorf("failed to update function app: %w", err)
	}
	return nil
}

// DeleteFunctionApp deletes a function app. Its deployments, routing tables, source directory and DNS record are
// removed asynchronously, so the app record is deleted before they are gone.
func (c *Composer) DeleteFunctionApp(appID string) error {
	app, err := c.functionAppRepo.GetByID(appID)
	if err != nil || app == nil {
		return fmt.Errorf("function app with id %s does not exist", appID)
	}

	// delete all deployments of the app
	deployments, err := c.deploymentRepo.GetByFunctionAppID(appID)
	if err != nil {
		return fmt.Errorf("failed to get deployments for function app %s: %w", appID, err)
	}

	for _, d := range deployments {

		go func(deploymentID string, sourcePath string) {
			resultChan := c.scheduler.AddTask(c.deleteTask(*d), MaxRetries)

			r := <-resultChan
			if r.Err != nil {
				log.Errorf("Deleting deployment with id %v failed: %v", deploymentID, r.Err)
			}
			log.Infof("Successfully deleted deployment with id %v", deploymentID)
			err = filesystem.DeleteDir(sourcePath)
			if err != nil {
				log.Errorf("could not delete app source directory: %s", err.Error())
			}

			err = c.routingClient.DeleteRoutingTable(deploymentID)
			if err != nil {
				log.Errorf("failed to delete routing table for deployment %s: %v", deploymentID, err)
			}
		}(d.Id, app.SourcePath)
	}

	if len(deployments) > 0 {
		go func() {
			if err := c.dnsClient.DeleteDNSRecord(context.TODO(), deployments[0].Namespace, app.Id); err != nil {
				log.Errorf("failed to delete DNS record for app %s: %v", app.Id, err)
			}
		}()
	}

	if err := c.functionAppRepo.Delete(appID); err != nil {
		return fmt.Errorf("failed to delete function app: %w", err)
	}
	return nil
}

// RollbackBulk deletes the records of a failed bulk creation: the given deployments, compositions and app. It only
// removes the database records; started builds and deployments are not undone.
func (c *Composer) RollbackBulk(
	app *FunctionApp,
	compositions []*FunctionComposition,
	deployments []*Deployment,
) {
	for _, dep := range deployments {
		if err := c.deploymentRepo.Delete(dep.Id); err != nil {
			log.Errorf("Failed to rollback deployment %s: %v", dep.Id, err)
		}
	}

	for _, fc := range compositions {
		if err := c.fcRepo.Delete(fc.Id); err != nil {
			log.Errorf("Failed to rollback function composition %s: %v", fc.Id, err)
		}
	}

	if app != nil {
		if err := c.functionAppRepo.Delete(app.Id); err != nil {
			log.Errorf("Failed to rollback function app %s: %v", app.Id, err)
		}
	}
}

// SetDeploymentMinScale changes how many warm replicas a deployment keeps
func (c *Composer) SetDeploymentMinScale(dep *Deployment, minScale int) error {
	if err := c.scaleClient.SetMinScale(context.TODO(), dep.Namespace, dep.Id, minScale); err != nil {
		return err
	}
	dep.Scale.MinReplicas = minScale
	return nil
}

// DeploymentReadyPods returns how many pods of a deployment are ready to serve
func (c *Composer) DeploymentReadyPods(dep *Deployment) (int, error) {
	return c.scaleClient.ReadyPods(context.TODO(), dep.Namespace, dep.Id)
}

// RepairDeploymentRevision replaces the broken revision of a deployment and reports whether it did
func (c *Composer) RepairDeploymentRevision(dep *Deployment) (bool, error) {
	return c.scaleClient.RepairRevision(context.TODO(), dep.Namespace, dep.Id)
}

// UpdateDNSRecord points the DNS record of an app's entry point to the given deployment.
func (c *Composer) UpdateDNSRecord(appID, namespace, targetDeploymentID string) error {
	return c.dnsClient.EnsureDNSRecord(context.TODO(), namespace, appID, targetDeploymentID)
}

// --- FUNCTION COMPOSITIONS ---

// AddFunctionComposition adds a function composition of the given components to an app. With an image the
// composition is marked as built; otherwise its build is started asynchronously.
func (c *Composer) AddFunctionComposition(appID string, components []string, image string) (*FunctionComposition, error) {
	fcApp, err := c.functionAppRepo.GetByID(appID)
	if err != nil || fcApp == nil {
		log.Errorf("function app with id %s does not exist", appID)
		return nil, fmt.Errorf("function app with id %s does not exist", appID)
	}

	id := uuid.New()

	// Collect all unique files from the selected components
	fileSet := make(map[string]struct{})
	for _, compName := range components {
		for _, comp := range fcApp.Components {
			if comp.Name == compName {
				for _, f := range comp.Files {
					fileSet[f] = struct{}{}
				}
			}
		}
	}
	var files []string
	for f := range fileSet {
		files = append(files, f)
	}

	fc := &FunctionComposition{
		Id:            "fc-" + id,
		FunctionAppId: appID,
		Components:    components,
		Files:         files,
		Status:        BuildStatusPending,
	}

	if image != "" {
		fc.Image = image
		fc.Timestamp = createBuildTimestamp()
		fc.Status = BuildStatusBuilt
	} else {
		go func() {
			resultChan := c.scheduler.AddTask(c.buildTask(*fc, fcApp.Runtime, fcApp.SourcePath), MaxRetries)
			r := <-resultChan
			if r.Err != nil {
				log.Errorf("Building of function composition with id %v failed: %v", fc.Id, r.Err)
				fc.Status = BuildStatusError
				if err := c.fcRepo.Save(fc); err != nil {
					log.Errorf("Failed to save function composition with id %s: %v", fc.Id, err)
				}
				return
			}
			log.Infof("Successfully submitted build for function composition with id %v", fc.Id)
		}()
	}

	if err := c.fcRepo.Save(fc); err != nil {
		return nil, fmt.Errorf("failed to save function composition: %w", err)
	}

	return fc, nil
}

// DeleteFunctionComposition deletes the deployments of a function composition and then the composition itself,
// asynchronously. If a deployment cannot be deleted, the composition is kept and its status is set to error.
func (c *Composer) DeleteFunctionComposition(fcID string) error {
	fc, err := c.fcRepo.GetByID(fcID)
	if err != nil || fc == nil {
		return fmt.Errorf("function composition with id %s does not exist", fcID)
	}

	go func() {
		for _, deployment := range fc.Deployments {
			resultChan := c.scheduler.AddTask(c.deleteTask(*deployment), MaxRetries)
			r := <-resultChan
			if r.Err != nil {
				log.Errorf("Deleting of deployment with id %v failed: %v", deployment.Id, r.Err)
				fc.Status = BuildStatusError
				if err := c.fcRepo.Save(fc); err != nil {
					log.Errorf("Failed to save function composition with id %s: %v", fc.Id, err)
				}
				return
			}
			log.Infof("Successfully deleted deployment with id %v", deployment.Id)
		}
		log.Infof("Successfully deleted all deployments for function composition with id %v", fc.Id)
		if err := c.fcRepo.Delete(fcID); err != nil {
			log.Errorf("failed to delete function composition: %w", err)
		}
	}()

	return nil
}

// --- DEPLOYMENTS ---

// CreateFcDeployment creates a deployment of a function composition and stores its routing table; a missing scale
// or missing resources get the default values. A deployment of a composition that is not built yet starts when
// NotifyBuildReady reports the image. The returned channel delivers the deployment result in both cases.
func (c *Composer) CreateFcDeployment(fcID, namespace, node string, routingTable RoutingTable, scale Scale, resources Resources) (*Deployment, <-chan Result, error) {
	fc, err := c.fcRepo.GetByID(fcID)
	if err != nil || fc == nil {
		return nil, nil, fmt.Errorf("function composition with id %s does not exist", fcID)
	}

	if scale.MinReplicas == 0 && scale.MaxReplicas == 0 && scale.TargetConcurrency == 0 {
		log.Warnf("Scale was not provided for deployment of function composition %s, using default values", fcID)
		scale = DefaultScaleValues()
	}
	if resources.Memory == 0 && resources.CPU == 0 {
		log.Warnf("Resources were not provided for deployment of function composition %s, using default values", fcID)
		resources = DefaultResources()
	}

	deployment := Deployment{
		Id:                    "d-" + uuid.New(),
		FunctionCompositionId: fcID,
		Namespace:             namespace,
		Node:                  node,
		RoutingTable:          routingTable,
		Scale:                 scale,
		Resources:             resources,
	}

	var resultChan <-chan Result
	if fc.Status == BuildStatusBuilt {
		deployment.Status = DeploymentStatusPending
		resultChan = c.startDeployment(&deployment, fc)
	} else {
		deployment.Status = DeploymentStatusWaitingForBuild
		log.Infof("Function composition with id %s is not built yet, deployment will be started after build is ready", fcID)
		ch := make(chan Result, 1)
		resultChan = ch

		c.mu.Lock()
		defer c.mu.Unlock()
		if c.pendingDeployments == nil {
			c.pendingDeployments = make(map[string]chan Result)
		}
		c.pendingDeployments[deployment.Id] = ch
	}

	err = c.routingClient.SetRoutingTable(deployment)
	if err != nil {
		log.Errorf("failed to set routing table for deployment %s: %v", deployment.Id, err)
		return nil, nil, err
	}

	err = c.deploymentRepo.Save(&deployment)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to save deployment: %w", err)
	}

	return &deployment, resultChan, nil
}

// DeleteFcDeployment deletes the routing table and the record of a deployment and schedules the removal of its
// Knative service. The returned channel delivers the result of the removal.
func (c *Composer) DeleteFcDeployment(deploymentID string) (<-chan Result, error) {
	deployment, err := c.deploymentRepo.GetByID(deploymentID)
	if err != nil || deployment == nil {
		return nil, fmt.Errorf("deployment with id %s does not exist", deploymentID)
	}

	err = c.routingClient.DeleteRoutingTable(deploymentID)
	if err != nil {
		return nil, fmt.Errorf("failed to delete routing table: %w", err)
	}

	err = c.deploymentRepo.Delete(deploymentID)
	if err != nil {
		return nil, fmt.Errorf("failed to delete deployment: %w", err)
	}

	resultChan := c.scheduler.AddTask(c.deleteTask(*deployment), MaxRetries)
	return resultChan, nil
}

// SetRoutingTable replaces the routing table of a deployment in the routing-table store and in the database.
func (c *Composer) SetRoutingTable(deploymentID string, table RoutingTable) error {
	deployment, err := c.deploymentRepo.GetByID(deploymentID)
	if err != nil || deployment == nil {
		return fmt.Errorf("deployment with id %s does not exist", deploymentID)
	}

	err = c.setRoutingTable(deployment, table)
	if err != nil {
		log.Errorf("failed to set routing table for deployment %s: %v", deploymentID, err)
		return err
	}
	return nil
}

// NotifyBuildReady handles the build notification of a function composition: it records the image, or the error
// status of a failed build, and starts the deployments that were waiting for the build.
func (c *Composer) NotifyBuildReady(fcID, imageURL string, status string) {
	c.builder.NotifyBuildFinished()
	fc, err := c.fcRepo.GetByID(fcID)
	if err != nil || fc == nil {
		log.Errorf("build notify failed, fc with id %s does not exist", fcID)
		return
	}

	fcApp, err := c.functionAppRepo.GetByID(fc.FunctionAppId)
	if err != nil || fcApp == nil {
		log.Errorf("function app with id %s does not exist", fc.FunctionAppId)
		return
	}

	// If the build failed, set status to failed
	if strings.ToLower(status) == "failed" {
		log.Errorf("Build for function composition %s failed", fcID)
		fc.Status = BuildStatusError
		if err := c.fcRepo.Save(fc); err != nil {
			log.Errorf("Failed to save function composition with id %s: %v", fc.Id, err)
		}
		return
	}

	fc.Image = imageURL
	fc.Timestamp = createBuildTimestamp()
	fc.Status = BuildStatusBuilt
	if err := c.fcRepo.Save(fc); err != nil {
		log.Errorf("Failed to save function composition with id %s: %v", fc.Id, err)
		return
	}
	log.Infof("Successfully built function composition with id %v. Image: %v", fc.Id, fc.Image)

	deployments, err := c.deploymentRepo.GetByFunctionCompositionID(fc.Id)
	if err != nil {
		log.Errorf("Failed to get deployments for function composition with id %s: %v", fc.Id, err)
		return
	}

	// Trigger any pending deployments, while also notifying services waiting for deployment result through channels
	for _, deployment := range deployments {
		if deployment.Status == DeploymentStatusWaitingForBuild {
			c.mu.Lock()
			defer c.mu.Unlock()
			ch, ok := c.pendingDeployments[deployment.Id]
			if ok {
				delete(c.pendingDeployments, deployment.Id)

				deployment.Status = DeploymentStatusPending
				depChan := c.startDeployment(deployment, fc)

				go func(origChan chan Result, depChan <-chan Result) {
					r := <-depChan
					origChan <- r
					close(origChan)
				}(ch, depChan)
			}

		}
	}
}

// --- INTERNAL METHODS ---

func (c *Composer) setRoutingTable(deployment *Deployment, table RoutingTable) error {
	deployment.RoutingTable = table
	err := c.routingClient.SetRoutingTable(*deployment)
	if err != nil {
		return fmt.Errorf("could not set routing table for deployment: %s, %s", deployment.Id, err.Error())
	}

	if err := c.deploymentRepo.Save(deployment); err != nil {
		return fmt.Errorf("failed to update deployment: %w", err)
	}

	return nil
}

func (c *Composer) startDeployment(deployment *Deployment, fc *FunctionComposition) <-chan Result {
	resultChan := c.scheduler.AddTask(c.deployTask(*deployment, fc.Image, fc.FunctionAppId), MaxRetries)
	go func(dep *Deployment, fc *FunctionComposition) {
		r := <-resultChan
		if r.Err != nil {
			log.Errorf("Deploying of function composition with id %v and deploymentId %v failed: %v, ", fc.Id, deployment.Id, r.Err)
			deployment.Status = DeploymentStatusError
			if err := c.deploymentRepo.Save(deployment); err != nil {
				log.Errorf("Failed to save deployment with id %s: %v", deployment.Id, err)
			}
			return
		}
		log.Infof("Successfully deployed function composition with id %v, deploymentId %v", fc.Id, deployment.Id)
		deployment.Status = DeploymentStatusDeployed
		if err := c.deploymentRepo.Save(deployment); err != nil {
			log.Errorf("Failed to save deployment with id %s: %v", deployment.Id, err)
		}
	}(deployment, fc)
	return resultChan
}

func (c *Composer) buildTask(fc FunctionComposition, runtime, sourcePath string) func() (interface{}, error) {
	return func() (interface{}, error) {
		buildDir, err := c.knClient.Init(context.TODO(), fc, runtime, sourcePath)
		if err != nil {
			return nil, fmt.Errorf("failed to init build: %v", err)
		}
		err = c.builder.Build(context.TODO(), fc, buildDir)
		if err != nil {
			return nil, fmt.Errorf("failed to build image: %v", err)
		}
		return fc, nil
	}
}

func (c *Composer) deployTask(deployment Deployment, image, appID string) func() (interface{}, error) {
	return func() (interface{}, error) {
		return nil, c.knClient.Deploy(context.TODO(), deployment, image, appID)
	}
}

func (c *Composer) deleteTask(deployment Deployment) func() (interface{}, error) {
	return func() (interface{}, error) {
		return nil, c.knClient.Delete(context.TODO(), deployment)
	}
}

// --- HELPERS ---

func createBuildTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// ComponentModuleName returns the file/module name of a component. Component
// names may contain hyphens, which are not valid in Python module names.
func ComponentModuleName(componentName string) string {
	return strings.ReplaceAll(componentName, "-", "_")
}

func isComponent(fileName string, runtime string, components []Component) bool {
	extension := filepath.Ext(fileName)
	if runtimeExtensions[runtime] != extension {
		return false
	}

	moduleName := strings.TrimSuffix(fileName, extension)
	for _, c := range components {
		if ComponentModuleName(c.Name) == moduleName {
			return true
		}
	}
	return false
}

// Default scale and resources of a deployment created without them.
const (
	DefaultMinReplicas       = 1
	DefaultMaxReplicas       = 3
	DefaultTargetConcurrency = 2
	DefaultMemoryMB          = 256
	DefaultCPUmCores         = 250
)

// DefaultScaleValues returns the scale of a deployment created without one.
func DefaultScaleValues() Scale {
	return Scale{
		MinReplicas:       DefaultMinReplicas,
		MaxReplicas:       DefaultMaxReplicas,
		TargetConcurrency: DefaultTargetConcurrency,
	}
}

// DefaultResources returns the resources of a deployment created without them.
func DefaultResources() Resources {
	return Resources{
		Memory: DefaultMemoryMB,
		CPU:    DefaultCPUmCores,
	}
}
