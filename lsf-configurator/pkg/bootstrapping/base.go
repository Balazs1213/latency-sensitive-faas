package bootstrapping

import "lsf-configurator/pkg/core"

// BaseBootstrapper holds the fields shared by the runtime-specific bootstrappers: the function composition, its
// build directory and the source directory of its app.
type BaseBootstrapper struct {
	fc         core.FunctionComposition
	buildDir   string
	sourcePath string
}
