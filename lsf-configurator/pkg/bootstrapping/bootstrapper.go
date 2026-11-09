// Package bootstrapping prepares the build directory of a function composition for its runtime: it copies the
// component files into the template and adapts the template to them.
package bootstrapping

// Bootstrapper prepares the build directory of a function composition for one runtime.
type Bootstrapper interface {
	Setup() error
}
