package core

type FunctionAppRepository interface {
	Save(app *FunctionApp) error
	GetByID(id string) (*FunctionApp, error)
	GetAll() ([]*FunctionApp, error)
	Delete(id string) error
}

type FunctionCompositionRepository interface {
	Save(comp *FunctionComposition) error
	GetByID(id string) (*FunctionComposition, error)
	Delete(id string) error
}

type DeploymentRepository interface {
	Save(deployment *Deployment) error
	GetByID(id string) (*Deployment, error)
	GetByFunctionCompositionID(functionCompositionID string) ([]*Deployment, error)
	GetByFunctionAppID(functionAppID string) ([]*Deployment, error)
	Delete(id string) error
}
