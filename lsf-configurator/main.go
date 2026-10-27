package main

import (
	"context"
	"errors"
	"io"
	"log"
	"lsf-configurator/api"
	"lsf-configurator/pkg/builder"
	"lsf-configurator/pkg/config"
	"lsf-configurator/pkg/core"
	"lsf-configurator/pkg/data/db"
	"lsf-configurator/pkg/data/repos"
	"lsf-configurator/pkg/filesystem"
	"lsf-configurator/pkg/knative"
	"lsf-configurator/pkg/kubeclient"
	"lsf-configurator/pkg/layout"
	"lsf-configurator/pkg/metrics"
	"lsf-configurator/pkg/results"
	"lsf-configurator/pkg/routing"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var controller core.Controller
var composer *core.Composer
var conf config.Configuration
var metricsReader core.MetricsReader
var resultsClient core.ResultsClient

func main() {
	logFile := configureLogging()
	defer logFile.Close()

	conf = config.Init()
	if !conf.VerboseLogs {
		disableStdOut()
	}

	knClient := knative.NewClient(conf)
	routingClient := routing.NewRouteConfigurator(conf.RedisUrl)
	tektonConf := builder.TektonConfig{
		Namespace:      conf.TektonNamespace,
		Pipeline:       conf.TektonPipeline,
		NotifyURL:      conf.TektonNotifyURL,
		WorkspacePVC:   conf.TektonWorkspacePVC,
		ImageRegistry:  conf.ImageRegistry,
		ImageRepo:      conf.ImageRepository,
		ServiceAccount: conf.TektonServiceAccount,
	}
	tektonBuilder := builder.NewTektonBuilder(tektonConf, conf.TektonConcurrencyLimit)

	db, err := db.InitDB(conf.DatabasePath)
	log.Printf("Database initialized successfully at path: %s", conf.DatabasePath)

	functionAppRepo := repos.NewFunctionAppRepository(db)
	fcRepo := repos.NewFunctionCompositionRepository(db)
	deploymentRepo := repos.NewDeploymentRepository(db)

	if err != nil {
		log.Fatalf("failed to initialize database: %v", err)
	}

	metricsReader, err = metrics.NewMetricsReader(conf.MetricsBackendAddress)
	if err != nil {
		log.Fatalf("failed to create metrics reader: %v", err)
	}

	dnsClient, err := kubeclient.NewKubeclient()
	if err != nil {
		log.Fatalf("failed to create DNS client: %v", err)
	}

	composer = core.NewComposer(functionAppRepo, fcRepo, deploymentRepo, routingClient,
		knClient, tektonBuilder, metricsReader, dnsClient, dnsClient)

	err = filesystem.CreateDir(conf.UploadDir)
	if err != nil {
		log.Fatalf("Could not create uploads directory: %v", err)
	}

	layoutCalculator := layout.NewLayoutCalculator(conf.PythonPath, conf.LayoutScriptPath, conf.PlatformNodes, conf.PlatformDelayMs)
	scenarioManager := core.NewScenarioManager(layoutCalculator, conf.TargetConcurrency,
		conf.InvocationSharedMemoryRatio, conf.ComponentMCPUAllocation, conf.OverheadMCPUAllocation, conf.TargetUtilization, conf.MemorySafetyBufferRatio)

	controllerCtx, controllerCancel := context.WithCancel(context.Background())
	controller = core.NewController(composer, metricsReader, scenarioManager,
		time.Duration(conf.ControllerTickDelaySeconds)*time.Second, conf.DeployNamespace,
		conf.AvailableNodeMemoryGb, core.MetricType(conf.ControllerMetricType),
		conf.ControllerMetricQueryTimeRange, conf.LatencyDowngradeFactor, conf.PrewarmMode, conf.TargetUtilization, conf.BoostRateFactor)

	if !conf.LocalMode {
		go func() {
			if err := controller.Start(controllerCtx); err != nil {
				log.Printf("Latency controller stopped with error: %v", err)
			} else {
				log.Println("Latency controller stopped gracefully")
			}
		}()
	}

	resultsClient = results.NewRedisResultsClient(conf.RedisUrl)

	s := startHttpServer()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	<-signalCh
	controllerCancel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		log.Fatalf("Shutdown error: %v", err)
	}
}

func startHttpServer() *http.Server {
	mux := http.NewServeMux()
	s := &http.Server{Addr: "0.0.0.0:8080", Handler: api.CorsMiddleware(mux)}
	registerHandlers(mux)

	go func() {
		log.Printf("Server listening on port 8080")
		if err := s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %s\n", err)
		}
	}()

	return s
}

func registerHandlers(mux *http.ServeMux) {
	mux.HandleFunc(api.HealthzPath, api.HealthCheckHandler)
	mux.Handle(api.AppsPath+"/", http.StripPrefix(api.AppsPath, api.NewHandlerApps(composer, controller, conf)))
	mux.Handle(api.DeploymentsPath+"/", http.StripPrefix(api.DeploymentsPath, api.NewHandlerDeployments(composer, conf)))
	mux.Handle(api.FunctionCompositionsPath+"/", http.StripPrefix(api.FunctionCompositionsPath, api.NewHandlerFunctionCompositions(composer, conf)))
	mux.Handle(api.MetricsPath+"/", http.StripPrefix(api.MetricsPath, api.NewHandlerMetrics(metricsReader, conf)))
	mux.Handle(api.ResultsPath+"/", http.StripPrefix(api.ResultsPath, api.NewHandlerResults(resultsClient)))
	mux.Handle("/", api.SpaHandler("./public"))
}

func configureLogging() *os.File {
	f, err := os.OpenFile("app.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)

	return f
}

// needed so knative library does not write stdout into docker logs
func disableStdOut() {
	null, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err == nil {
		os.Stdout = null
		os.Stderr = null
	}
}
