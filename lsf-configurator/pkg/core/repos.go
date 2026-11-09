package core

// FunctionAppRepository persists function apps.
type FunctionAppRepository interface {
	Save(app *FunctionApp) error
	GetByID(id string) (*FunctionApp, error)
	GetAll() ([]*FunctionApp, error)
	Delete(id string) error
}

// FunctionCompositionRepository persists function compositions.
type FunctionCompositionRepository interface {
	Save(comp *FunctionComposition) error
	GetByID(id string) (*FunctionComposition, error)
	Delete(id string) error
}

// DeploymentRepository persists deployments and finds them by function composition or function app.
type DeploymentRepository interface {
	Save(deployment *Deployment) error
	GetByID(id string) (*Deployment, error)
	GetByFunctionCompositionID(functionCompositionID string) ([]*Deployment, error)
	GetByFunctionAppID(functionAppID string) ([]*Deployment, error)
	Delete(id string) error
}
