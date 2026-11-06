package repos

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"lsf-configurator/pkg/core"
	"sync"
)

var dbWriteMutex sync.Mutex

type functionAppRepo struct {
	db *sql.DB
}

func NewFunctionAppRepository(db *sql.DB) core.FunctionAppRepository {
	return &functionAppRepo{db: db}
}

func (r *functionAppRepo) Save(app *core.FunctionApp) error {
	dbWriteMutex.Lock()
	defer dbWriteMutex.Unlock()
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}
	// Rollback after a successful Commit returns sql.ErrTxDone, which is expected.
	defer func() { _ = tx.Rollback() }()

	componentsJSON, err := json.Marshal(app.Components)
	if err != nil {
		return fmt.Errorf("failed to marshal components: %w", err)
	}
	linksJSON, err := json.Marshal(app.Links)
	if err != nil {
		return fmt.Errorf("failed to marshal links: %w", err)
	}
	filesJSON, err := json.Marshal(app.Files)
	if err != nil {
		return fmt.Errorf("failed to marshal files: %w", err)
	}

	layoutJSON, err := json.Marshal(app.LayoutCandidates)
	if err != nil {
		return fmt.Errorf("failed to marshal layout candidates: %w", err)
	}

	_, err = tx.Exec(`
		INSERT OR REPLACE INTO function_apps (id, name, runtime, components, links, files, source_path, latency_limit, layout_candidates, active_layout_key) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		app.Id, app.Name, app.Runtime, string(componentsJSON), string(linksJSON),
		string(filesJSON), app.SourcePath, app.LatencyLimit, string(layoutJSON), app.ActiveLayoutKey)
	if err != nil {
		return err
	}

	compRepo := NewFunctionCompositionRepository(r.db).(*functionCompositionRepo)

	// Save each composition
	for _, comp := range app.Compositions {
		err := compRepo.saveWithTx(tx, comp)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *functionAppRepo) GetByID(id string) (*core.FunctionApp, error) {
	row := r.db.QueryRow(`
	SELECT id, name, runtime, components, links, files, source_path, latency_limit, layout_candidates, active_layout_key
	FROM function_apps WHERE id = ?`, id)

	var app core.FunctionApp
	var componentsJSON, linksJSON, filesJSON, sourcePath string
	var latencyLimit int
	var activeLayoutKey string
	var layoutCandidatesJSON string

	if err := row.Scan(&app.Id, &app.Name, &app.Runtime, &componentsJSON,
		&linksJSON, &filesJSON, &sourcePath, &latencyLimit, &layoutCandidatesJSON, &activeLayoutKey); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	if err := json.Unmarshal([]byte(componentsJSON), &app.Components); err != nil {
		return nil, fmt.Errorf("failed to parse components: %w", err)
	}
	if err := json.Unmarshal([]byte(linksJSON), &app.Links); err != nil {
		return nil, fmt.Errorf("failed to parse links: %w", err)
	}
	if err := json.Unmarshal([]byte(filesJSON), &app.Files); err != nil {
		return nil, fmt.Errorf("failed to parse files: %w", err)
	}
	if err := json.Unmarshal([]byte(layoutCandidatesJSON), &app.LayoutCandidates); err != nil {
		return nil, fmt.Errorf("failed to parse layout candidates: %w", err)
	}
	app.SourcePath = sourcePath
	app.LatencyLimit = latencyLimit
	app.ActiveLayoutKey = activeLayoutKey

	app.Compositions = make([]*core.FunctionComposition, 0)
	rows, err := r.db.Query(`SELECT id FROM function_compositions WHERE function_app_id = ?`, app.Id)
	if err != nil {
		return nil, err
	}
	// Read-only query, so a failed close cannot affect the result.
	defer func() { _ = rows.Close() }()

	compRepo := NewFunctionCompositionRepository(r.db)
	for rows.Next() {
		var compID string
		if err := rows.Scan(&compID); err != nil {
			return nil, err
		}
		comp, err := compRepo.GetByID(compID)
		if err != nil {
			return nil, err
		}
		app.Compositions = append(app.Compositions, comp)
	}

	return &app, nil
}

func (r *functionAppRepo) GetAll() ([]*core.FunctionApp, error) {
	rows, err := r.db.Query(`
	SELECT id, name, runtime, components, links, files, source_path, latency_limit, layout_candidates, active_layout_key
	FROM function_apps`)
	if err != nil {
		return nil, err
	}
	// Read-only query, so a failed close cannot affect the result.
	defer func() { _ = rows.Close() }()

	var apps []*core.FunctionApp
	for rows.Next() {
		var app core.FunctionApp
		var componentsJSON, linksJSON, filesJSON, sourcePath string
		var latencyLimit int
		var layoutCandidatesJSON, activeLayoutKey string
		if err := rows.Scan(&app.Id, &app.Name, &app.Runtime, &componentsJSON,
			&linksJSON, &filesJSON, &sourcePath, &latencyLimit, &layoutCandidatesJSON, &activeLayoutKey); err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(componentsJSON), &app.Components); err != nil {
			return nil, fmt.Errorf("failed to parse components: %w", err)
		}
		if err := json.Unmarshal([]byte(linksJSON), &app.Links); err != nil {
			return nil, fmt.Errorf("failed to parse links: %w", err)
		}
		if err := json.Unmarshal([]byte(layoutCandidatesJSON), &app.LayoutCandidates); err != nil {
			return nil, fmt.Errorf("failed to parse layout candidates: %w", err)
		}
		if err := json.Unmarshal([]byte(filesJSON), &app.Files); err != nil {
			return nil, fmt.Errorf("failed to parse files: %w", err)
		}
		app.SourcePath = sourcePath
		app.LatencyLimit = latencyLimit
		app.ActiveLayoutKey = activeLayoutKey

		apps = append(apps, &app)
	}

	return apps, nil
}

func (r *functionAppRepo) Delete(id string) error {
	dbWriteMutex.Lock()
	defer dbWriteMutex.Unlock()
	_, err := r.db.Exec(`DELETE FROM function_apps WHERE id = ?`, id)
	return err
}

type functionCompositionRepo struct {
	db *sql.DB
}

func NewFunctionCompositionRepository(db *sql.DB) core.FunctionCompositionRepository {
	return &functionCompositionRepo{db: db}
}

func (r *functionCompositionRepo) Save(comp *core.FunctionComposition) error {
	dbWriteMutex.Lock()
	defer dbWriteMutex.Unlock()
	filesJSON, err := json.Marshal(comp.Files)
	if err != nil {
		return fmt.Errorf("failed to marshal files: %w", err)
	}

	componentsJSON, err := json.Marshal(comp.Components)
	if err != nil {
		return fmt.Errorf("failed to marshal components: %w", err)
	}

	_, err = r.db.Exec(`
		INSERT OR REPLACE INTO function_compositions (
			id, function_app_id,
			image, timestamp, files, components, status
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		comp.Id, comp.FunctionAppId, comp.Image,
		comp.Timestamp, string(filesJSON), string(componentsJSON), comp.Status,
	)

	return err
}

func (r *functionCompositionRepo) GetByID(id string) (*core.FunctionComposition, error) {
	row := r.db.QueryRow(`
		SELECT id, function_app_id, image, timestamp, files, components, status
		FROM function_compositions
		WHERE id = ?`, id)

	var comp core.FunctionComposition
	var filesJSON, componentsJSON string

	err := row.Scan(
		&comp.Id, &comp.FunctionAppId, &comp.Image, &comp.Timestamp,
		&filesJSON, &componentsJSON, &comp.Status,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	if err := json.Unmarshal([]byte(filesJSON), &comp.Files); err != nil {
		return nil, fmt.Errorf("failed to parse files: %w", err)
	}
	if err := json.Unmarshal([]byte(componentsJSON), &comp.Components); err != nil {
		return nil, fmt.Errorf("failed to parse components: %w", err)
	}

	// Query related deployments
	deploymentRepo := NewDeploymentRepository(r.db)
	deployments, err := deploymentRepo.GetByFunctionCompositionID(comp.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to query deployments for function composition %s: %w", comp.Id, err)
	}
	comp.Deployments = deployments

	return &comp, nil
}

func (r *functionCompositionRepo) Delete(id string) error {
	dbWriteMutex.Lock()
	defer dbWriteMutex.Unlock()
	_, err := r.db.Exec(`DELETE FROM function_compositions WHERE id = ?`, id)
	return err
}

func (r *functionCompositionRepo) saveWithTx(tx *sql.Tx, comp *core.FunctionComposition) error {
	filesJSON, err := json.Marshal(comp.Files)
	if err != nil {
		return fmt.Errorf("failed to marshal files: %w", err)
	}
	componentsJSON, err := json.Marshal(comp.Components)
	if err != nil {
		return fmt.Errorf("failed to marshal components: %w", err)
	}

	_, err = tx.Exec(`
		INSERT OR REPLACE INTO function_compositions (
			id, function_app_id,
			image, timestamp, files, components, status
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		comp.Id, comp.FunctionAppId, comp.Image, comp.Timestamp,
		string(filesJSON), string(componentsJSON), comp.Status,
	)
	return err
}

type deploymentRepo struct {
	db *sql.DB
}

func NewDeploymentRepository(db *sql.DB) *deploymentRepo {
	return &deploymentRepo{db: db}
}

func (r *deploymentRepo) Save(deployment *core.Deployment) error {
	dbWriteMutex.Lock()
	defer dbWriteMutex.Unlock()

	routingTableJSON, err := json.Marshal(deployment.RoutingTable)
	if err != nil {
		return fmt.Errorf("failed to marshal routing table: %w", err)
	}

	_, err = r.db.Exec(`
		INSERT OR REPLACE INTO deployments (
			id, function_composition_id, node, namespace, routing_table, status, scale_min_replicas, scale_max_replicas, scale_target_concurrency, resources_memory, resources_cpu
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		deployment.Id, deployment.FunctionCompositionId, deployment.Node,
		deployment.Namespace, string(routingTableJSON), deployment.Status,
		deployment.Scale.MinReplicas, deployment.Scale.MaxReplicas,
		deployment.Scale.TargetConcurrency, deployment.Resources.Memory, deployment.Resources.CPU,
	)
	return err
}

func (r *deploymentRepo) Delete(id string) error {
	dbWriteMutex.Lock()
	defer dbWriteMutex.Unlock()

	_, err := r.db.Exec(`DELETE FROM deployments WHERE id = ?`, id)
	return err
}

func (r *deploymentRepo) GetByID(id string) (*core.Deployment, error) {
	row := r.db.QueryRow(`
		SELECT id, function_composition_id, node, namespace, routing_table, status, scale_min_replicas, scale_max_replicas, scale_target_concurrency, resources_memory, resources_cpu
		FROM deployments
		WHERE id = ?`, id)

	var deployment core.Deployment
	var routingTableJSON string

	err := row.Scan(
		&deployment.Id, &deployment.FunctionCompositionId, &deployment.Node,
		&deployment.Namespace, &routingTableJSON, &deployment.Status,
		&deployment.Scale.MinReplicas, &deployment.Scale.MaxReplicas,
		&deployment.Scale.TargetConcurrency, &deployment.Resources.Memory, &deployment.Resources.CPU,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	if err := json.Unmarshal([]byte(routingTableJSON), &deployment.RoutingTable); err != nil {
		return nil, fmt.Errorf("failed to parse routing table: %w", err)
	}

	return &deployment, nil
}

func (r *deploymentRepo) GetByFunctionCompositionID(functionCompositionID string) ([]*core.Deployment, error) {
	rows, err := r.db.Query(`
		SELECT id, function_composition_id, node, namespace, routing_table, status, scale_min_replicas, scale_max_replicas, scale_target_concurrency, resources_memory, resources_cpu
		FROM deployments
		WHERE function_composition_id = ?`, functionCompositionID)
	if err != nil {
		return nil, err
	}
	// Read-only query, so a failed close cannot affect the result.
	defer func() { _ = rows.Close() }()

	deployments := make([]*core.Deployment, 0)
	for rows.Next() {
		var deployment core.Deployment
		var routingTableJSON string

		err := rows.Scan(
			&deployment.Id, &deployment.FunctionCompositionId, &deployment.Node,
			&deployment.Namespace, &routingTableJSON, &deployment.Status,
			&deployment.Scale.MinReplicas, &deployment.Scale.MaxReplicas,
			&deployment.Scale.TargetConcurrency, &deployment.Resources.Memory, &deployment.Resources.CPU,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(routingTableJSON), &deployment.RoutingTable); err != nil {
			return nil, fmt.Errorf("failed to parse routing table: %w", err)
		}

		deployments = append(deployments, &deployment)
	}

	return deployments, nil
}

func (r *deploymentRepo) GetByFunctionAppID(functionAppID string) ([]*core.Deployment, error) {
	rows, err := r.db.Query(`
		SELECT d.id, d.function_composition_id, d.node, d.namespace, d.routing_table, d.status, d.scale_min_replicas, d.scale_max_replicas, d.scale_target_concurrency, d.resources_memory, d.resources_cpu
		FROM deployments d
		INNER JOIN function_compositions fc ON d.function_composition_id = fc.id
		WHERE fc.function_app_id = ?`, functionAppID)
	if err != nil {
		return nil, err
	}
	// Read-only query, so a failed close cannot affect the result.
	defer func() { _ = rows.Close() }()

	var deployments []*core.Deployment
	for rows.Next() {
		var deployment core.Deployment
		var routingTableJSON string

		err := rows.Scan(
			&deployment.Id, &deployment.FunctionCompositionId, &deployment.Node,
			&deployment.Namespace, &routingTableJSON, &deployment.Status,
			&deployment.Scale.MinReplicas, &deployment.Scale.MaxReplicas,
			&deployment.Scale.TargetConcurrency, &deployment.Resources.Memory, &deployment.Resources.CPU,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(routingTableJSON), &deployment.RoutingTable); err != nil {
			return nil, fmt.Errorf("failed to parse routing table: %w", err)
		}

		deployments = append(deployments, &deployment)
	}

	return deployments, nil
}
