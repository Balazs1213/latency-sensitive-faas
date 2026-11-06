package bootstrapping

import "lsf-configurator/pkg/core"

type BaseBootstrapper struct {
	fc         core.FunctionComposition
	buildDir   string
	sourcePath string
}
