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

const (
	MaxRetries     = 3
	WorkerPoolSize = 10
	QueueSize      = 30
)

// for now, only python is implemented
var runtimeExtensions = map[string]string{
	"python": ".py", // Python
}

type Composer struct {
	knClient           KnClient
	scheduler          Scheduler
	routingClient      RoutingClient
	builder            Builder
	dnsClient          DNSClient
	functionAppRepo    FunctionAppRepository
	fcRepo             FunctionCompositionRepository
	deploymentRepo     DeploymentRepository
	metricsReader      MetricsReader
	pendingDeployments map[string]chan Result // key = deploymentId
	mu                 sync.Mutex
}

func NewComposer(
	functionAppRepo FunctionAppRepository,
	fcRepo FunctionCompositionRepository,
	deploymentRepo DeploymentRepository,
	routingClient RoutingClient,
	knClient KnClient,
	builder Builder,
	metricsReader MetricsReader,
	dnsClient DNSClient,
) *Composer {
	scheduler := NewScheduler(WorkerPoolSize, QueueSize)
	return &Composer{
		knClient:        knClient,
		scheduler:       scheduler,
		routingClient:   routingClient,
		builder:         builder,
		dnsClient:       dnsClient,
		functionAppRepo: functionAppRepo,
		fcRepo:          fcRepo,
		deploymentRepo:  deploymentRepo,
		metricsReader:   metricsReader,
	}
}

// --- FUNCTION APPS ---

func (c *Composer) GetFunctionApp(appId string) (*FunctionApp, error) {
	return c.functionAppRepo.GetByID(appId)
}

func (c *Composer) ListFunctionApps() ([]*FunctionApp, error) {
	return c.functionAppRepo.GetAll()
}

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

func (c *Composer) UpdateFunctionApp(app *FunctionApp) error {
	if app == nil {
		return fmt.Errorf("function app is nil")
	}
	if err := c.functionAppRepo.Save(app); err != nil {
		return fmt.Errorf("failed to update function app: %w", err)
	}
	return nil
}

func (c *Composer) DeleteFunctionApp(appId string) error {
	app, err := c.functionAppRepo.GetByID(appId)
	if err != nil || app == nil {
		return fmt.Errorf("function app with id %s does not exist", appId)
	}

	// delete all deployments of the app
	deployments, err := c.deploymentRepo.GetByFunctionAppID(appId)
	if err != nil {
		return fmt.Errorf("failed to get deployments for function app %s: %w", appId, err)
	}

	for _, d := range deployments {

		go func(deploymentId string, sourcePath string) {
			resultChan := c.scheduler.AddTask(c.deleteTask(*d), MaxRetries)

			r := <-resultChan
			if r.Err != nil {
				log.Errorf("Deleting deployment with id %v failed: %v", deploymentId, r.Err)
			}
			log.Infof("Successfully deleted deployment with id %v", deploymentId)
			err = filesystem.DeleteDir(sourcePath)
			if err != nil {
				log.Errorf("could not delete app source directory: %s", err.Error())
			}

			err = c.routingClient.DeleteRoutingTable(deploymentId)
			if err != nil {
				log.Errorf("failed to delete routing table for deployment %s: %v", deploymentId, err)
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

	if err := c.functionAppRepo.Delete(appId); err != nil {
		return fmt.Errorf("failed to delete function app: %w", err)
	}
	return nil
}

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

func (c *Composer) UpdateDNSRecord(appId, namespace, targetDeploymentId string) error {
	return c.dnsClient.EnsureDNSRecord(context.TODO(), namespace, appId, targetDeploymentId)
}

// --- FUNCTION COMPOSITIONS ---

func (c *Composer) AddFunctionComposition(appId string, components []string, image string) (*FunctionComposition, error) {
	fcApp, err := c.functionAppRepo.GetByID(appId)
	if err != nil || fcApp == nil {
		log.Errorf("function app with id %s does not exist", appId)
		return nil, fmt.Errorf("function app with id %s does not exist", appId)
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
		FunctionAppId: appId,
		Components:    components,
		Files:         files,
		Status:        BuildStatusPending,
	}

	if image != "" {
		fc.Build.Image = image
		fc.Build.Timestamp = createBuildTimestamp()
		fc.Status = BuildStatusBuilt
	} else {
		go func() {
			resultChan := c.scheduler.AddTask(c.buildTask(*fc, fcApp.Runtime, fcApp.SourcePath), MaxRetries)
			r := <-resultChan
			if r.Err != nil {
				log.Errorf("Building of function composition with id %v failed: %v", fc.Id, r.Err)
				fc.Status = BuildStatusError
				c.fcRepo.Save(fc)
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

func (c *Composer) DeleteFunctionComposition(fcId string) error {
	fc, err := c.fcRepo.GetByID(fcId)
	if err != nil || fc == nil {
		return fmt.Errorf("function composition with id %s does not exist", fcId)
	}

	go func() {
		for _, deployment := range fc.Deployments {
			resultChan := c.scheduler.AddTask(c.deleteTask(*deployment), MaxRetries)
			r := <-resultChan
			if r.Err != nil {
				log.Errorf("Deleting of deployment with id %v failed: %v", deployment.Id, r.Err)
				fc.Status = BuildStatusError
				c.fcRepo.Save(fc)
				return
			}
			log.Infof("Successfully deleted deployment with id %v", deployment.Id)
		}
		log.Infof("Successfully deleted all deployments for function composition with id %v", fc.Id)
		if err := c.fcRepo.Delete(fcId); err != nil {
			log.Errorf("failed to delete function composition: %w", err)
		}
	}()

	return nil
}

// --- DEPLOYMENTS ---

func (c *Composer) CreateFcDeployment(fcId, namespace, node string, routingTable RoutingTable, scale Scale, resources Resources) (*Deployment, <-chan Result, error) {
	fc, err := c.fcRepo.GetByID(fcId)
	if err != nil || fc == nil {
		return nil, nil, fmt.Errorf("function composition with id %s does not exist", fcId)
	}

	if scale.MinReplicas == 0 && scale.MaxReplicas == 0 && scale.TargetConcurrency == 0 {
		log.Warnf("Scale was not provided for deployment of function composition %s, using default values", fcId)
		scale = DefaultScaleValues()
	}
	if resources.Memory == 0 && resources.CPU == 0 {
		log.Warnf("Resources were not provided for deployment of function composition %s, using default values", fcId)
		resources = DefaultResources()
	}

	deployment := Deployment{
		Id:                    "d-" + uuid.New(),
		FunctionCompositionId: fcId,
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
		log.Infof("Function composition with id %s is not built yet, deployment will be started after build is ready", fcId)
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

func (c *Composer) DeleteFcDeployment(deploymentId string) (<-chan Result, error) {
	deployment, err := c.deploymentRepo.GetByID(deploymentId)
	if err != nil || deployment == nil {
		return nil, fmt.Errorf("deployment with id %s does not exist", deploymentId)
	}

	err = c.routingClient.DeleteRoutingTable(deploymentId)
	if err != nil {
		return nil, fmt.Errorf("failed to delete routing table: %w", err)
	}

	err = c.deploymentRepo.Delete(deploymentId)
	if err != nil {
		return nil, fmt.Errorf("failed to delete deployment: %w", err)
	}

	resultChan := c.scheduler.AddTask(c.deleteTask(*deployment), MaxRetries)
	return resultChan, nil
}

func (c *Composer) SetRoutingTable(deploymentId string, table RoutingTable) error {
	deployment, err := c.deploymentRepo.GetByID(deploymentId)
	if err != nil || deployment == nil {
		return fmt.Errorf("deployment with id %s does not exist", deploymentId)
	}

	err = c.setRoutingTable(deployment, table)
	if err != nil {
		log.Errorf("failed to set routing table for deployment %s: %v", deploymentId, err)
		return err
	}
	return nil
}

func (c *Composer) NotifyBuildReady(fcId, imageURL string, status string) {
	c.builder.NotifyBuildFinished()
	fc, err := c.fcRepo.GetByID(fcId)
	if err != nil || fc == nil {
		log.Errorf("build notify failed, fc with id %s does not exist", fcId)
		return
	}

	fcApp, err := c.functionAppRepo.GetByID(fc.FunctionAppId)
	if err != nil || fcApp == nil {
		log.Errorf("function app with id %s does not exist", fc.FunctionAppId)
		return
	}

	// If the build failed, set status to failed
	if strings.ToLower(status) == "failed" {
		log.Errorf("Build for function composition %s failed", fcId)
		fc.Status = BuildStatusError
		if err := c.fcRepo.Save(fc); err != nil {
			log.Errorf("Failed to save function composition with id %s: %v", fc.Id, err)
		}
		return
	}

	fc.Build.Image = imageURL
	fc.Build.Timestamp = createBuildTimestamp()
	fc.Status = BuildStatusBuilt
	if err := c.fcRepo.Save(fc); err != nil {
		log.Errorf("Failed to save function composition with id %s: %v", fc.Id, err)
		return
	}
	log.Infof("Successfully built function composition with id %v. Image: %v", fc.Id, fc.Build.Image)

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
	resultChan := c.scheduler.AddTask(c.deployTask(*deployment, fc.Build.Image, fc.FunctionAppId), MaxRetries)
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

func (c *Composer) deployTask(deployment Deployment, image, appId string) func() (interface{}, error) {
	return func() (interface{}, error) {
		return nil, c.knClient.Deploy(context.TODO(), deployment, image, appId)
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

const (
	DefaultMinReplicas       = 1
	DefaultMaxReplicas       = 3
	DefaultTargetConcurrency = 2
	DefaultMemoryMB          = 256
	DefaultCPUmCores         = 250
)

func DefaultScaleValues() Scale {
	return Scale{
		MinReplicas:       DefaultMinReplicas,
		MaxReplicas:       DefaultMaxReplicas,
		TargetConcurrency: DefaultTargetConcurrency,
	}
}

func DefaultResources() Resources {
	return Resources{
		Memory: DefaultMemoryMB,
		CPU:    DefaultCPUmCores,
	}
}
