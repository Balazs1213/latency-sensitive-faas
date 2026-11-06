package bootstrapping

type Bootstrapper interface {
	Setup() error
}
