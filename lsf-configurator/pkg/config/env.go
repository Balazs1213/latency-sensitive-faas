package config

import (
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Configuration struct {
	UploadDir                      string   `env:"UPLOAD_DIR"`
	TemplatesPath                  string   `env:"TEMPLATES_PATH"`
	ImageRegistry                  string   `env:"IMAGE_REGISTRY"`
	ImageRepository                string   `env:"IMAGE_REPOSITORY"`
	RedisUrl                       string   `env:"REDIS_URL"`
	VerboseLogs                    bool     `env:"VERBOSE_LOGS"`
	MetricsBackendAddress          string   `env:"METRICS_BACKEND_ADDRESS"`
	TektonNamespace                string   `env:"TEKTON_NAMESPACE"`
	TektonPipeline                 string   `env:"TEKTON_PIPELINE"`
	TektonNotifyURL                string   `env:"TEKTON_NOTIFY_URL"`
	TektonWorkspacePVC             string   `env:"TEKTON_WORKSPACE_PVC"`
	TektonServiceAccount           string   `env:"TEKTON_SERVICE_ACCOUNT"`
	TektonConcurrencyLimit         int      `env:"TEKTON_CONCURRENCY_LIMIT"`
	DatabasePath                   string   `env:"DATABASE_PATH"`
	AlertingApiUrl                 string   `env:"ALERTING_API_URL"`
	AlertingUsername               string   `env:"ALERTING_USERNAME"`
	AlertingPassword               string   `env:"ALERTING_PASSWORD"`
	AlertsIndex                    string   `env:"ALERTS_INDEX" default:"latency-alerts"`
	AlertingConnector              string   `env:"ALERTING_CONNECTOR" default:"lsf-alerts-connector"`
	LocalMode                      bool     `env:"LOCAL_MODE" default:"false"`
	DeployNamespace                string   `env:"DEPLOY_NAMESPACE" default:"application"`
	ControllerTickDelaySeconds     int      `env:"CONTROLLER_TICK_DELAY_SECONDS" default:"1"`
	ControllerMetricType           string   `env:"CONTROLLER_METRIC_TYPE" default:"AVG"`
	ControllerMetricQueryTimeRange string   `env:"CONTROLLER_METRIC_QUERY_TIME_RANGE" default:"now-5m"`
	PlatformNodes                  []string `env:"PLATFORM_NODES"`
	PlatformDelayMs                int      `env:"PLATFORM_DELAY_MS"`
	AvailableNodeMemoryGb          int      `env:"AVAILABLE_NODE_MEMORY_GB"`
	PythonPath                     string   `env:"PYTHON_PATH" default:"python3"`
	LayoutScriptPath               string   `env:"LAYOUT_SCRIPT_PATH"`
	ResultStoreAddress             string   `env:"RESULT_STORE_ADDRESS"`
	TargetConcurrency              int      `env:"TARGET_CONCURRENCY" default:"2"`
	ComponentMCPUAllocation        int      `env:"COMPONENT_MCPU_ALLOCATION" default:"500"`
	OverheadMCPUAllocation         int      `env:"OVERHEAD_MCPU_ALLOCATION" default:"0"`
	// Indicates the fraction of memory that is shared among concurrent invocations of the same component
	// E.g. 0.5 means that 50% of the memory is shared, and only the remaining 50% scales with the number concurrent invocations
	InvocationSharedMemoryRatio float64 `env:"COMPONENT_SHARED_MEMORY_RATIO" default:"0.5"`
	// Safety buffer as a percentage of the composition memory (e.g., 0.2 means 20% buffer)
	MemorySafetyBufferRatio float64 `env:"MEMORY_SAFETY_BUFFER_RATIO" default:"0.2"`
	// Target utilization ratio for replica calculations (e.g., 0.7 means keep utilization below 70% to avoid queueing delays)
	TargetUtilization      float64 `env:"TARGET_UTILIZATION" default:"0.7"`
	LatencyDowngradeFactor float64 `env:"LATENCY_DOWNGRADE_FACTOR" default:"0.5"`
	// Warm standby for the inactive layouts: "off" (deploy on switch), "one" (one warm pod), "full" (all required replicas warm)
	PrewarmMode string `env:"PREWARM_MODE" default:"off"`
	// Adaptive prewarm: boost the standby layout when the arrival rate exceeds this share of the active layout's capacity
	BoostRateFactor float64 `env:"BOOST_RATE_FACTOR" default:"0.75"`
}

func Init() Configuration {
	if err := godotenv.Load(); err != nil {
		log.Printf("No .env file found. Using system environment variables")
	}

	conf := Configuration{}
	confVal := reflect.ValueOf(&conf).Elem()
	confType := confVal.Type()

	for i := 0; i < confVal.NumField(); i++ {
		field := confType.Field(i)
		envVar := field.Tag.Get("env")

		if envVar == "" {
			continue // Skip fields without an env tag
		}

		envValue := os.Getenv(envVar)
		if envValue == "" {
			envValue = field.Tag.Get("default")
		}

		if envValue == "" {
			continue
		}

		fieldVal := confVal.Field(i)

		switch fieldVal.Kind() {
		case reflect.String:
			fieldVal.SetString(envValue)

		case reflect.Int:
			intValue, err := strconv.Atoi(envValue)
			if err == nil {
				fieldVal.SetInt(int64(intValue))
			} else {
				log.Fatalf("Invalid int value for %s: %s", envVar, envValue)
			}

		case reflect.Bool:
			boolValue, err := strconv.ParseBool(envValue)
			if err == nil {
				fieldVal.SetBool(boolValue)
			} else {
				log.Fatalf("Invalid bool value for %s: %s", envVar, envValue)
			}

		case reflect.Float64:
			floatValue, err := strconv.ParseFloat(envValue, 64)
			if err == nil {
				fieldVal.SetFloat(floatValue)
			} else {
				log.Fatalf("Invalid float64 value for %s: %s", envVar, envValue)
			}

		case reflect.Slice:
			if fieldVal.Type().Elem().Kind() == reflect.String {
				parts := strings.Split(envValue, ",")
				for i := range parts {
					parts[i] = strings.TrimSpace(parts[i])
				}
				fieldVal.Set(reflect.ValueOf(parts))
			} else {
				log.Fatalf("Unsupported slice type for %s", field.Name)
			}
		}
	}

	return conf
}
