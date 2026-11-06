package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"lsf-configurator/pkg/core"
	"net/http"
	"net/url"
	"strings"
)

type alertClient struct {
	alertApiUrl string
	username    string
	password    string
	connectorId string
}

func NewAlertClient(alertApiUrl, username, password string) core.AlertClient {
	return &alertClient{
		alertApiUrl: alertApiUrl,
		username:    username,
		password:    password,
		connectorId: "",
	}
}

func (c *alertClient) EnsureAlertConnector(ctx context.Context, connectorName, indexName string) (string, error) {
	// 1Check if connector exists
	url := fmt.Sprintf("%s/api/actions/connectors", c.alertApiUrl)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("kbn-xsrf", "true")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("failed to list connectors: %s", string(data))
	}

	var connectors []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &connectors); err != nil {
		return "", err
	}

	for _, conn := range connectors {
		if conn.Name == connectorName {
			c.connectorId = conn.ID
			return conn.ID, nil // connector exists
		}
	}

	// 2️⃣ Create connector if it does not exist
	body := fmt.Sprintf(`{
		"name": "%s",
		"connector_type_id": ".index",
		"config": { "index": "%s" },
		"secrets": {}
	}`, connectorName, indexName)

	req, _ = http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/api/actions/connector", c.alertApiUrl), strings.NewReader(body))
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("kbn-xsrf", "true")
	req.Header.Set("Content-Type", "application/json")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, _ = io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("failed to create connector: %s", string(data))
	}

	var respJSON struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &respJSON); err != nil {
		return "", err
	}

	log.Printf("Index connector %s created with ID %s", connectorName, respJSON.ID)

	c.connectorId = respJSON.ID
	return respJSON.ID, nil
}

func (c *alertClient) DeleteRule(ctx context.Context, ruleID string) error {
	delURL := fmt.Sprintf("%s/api/alerting/rule/%s", c.alertApiUrl, ruleID)
	req, err := http.NewRequestWithContext(ctx, "DELETE", delURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request: %w", err)
	}
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("kbn-xsrf", "true")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute delete request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete rule %s: %s", ruleID, string(body))
	}

	log.Printf("Deleted existing rule %s", ruleID)
	return nil
}

func (c *alertClient) CreateOrUpdateRule(ctx context.Context, serviceName string, latencyThresholdMs int) (string, error) {
	// Find existing rule
	existingID, err := c.FindRuleByServiceName(serviceName)
	if err != nil {
		return "", fmt.Errorf("failed to search existing rule: %w", err)
	}

	// Delete if it exists
	if existingID != "" {
		if err := c.DeleteRule(ctx, existingID); err != nil {
			return "", fmt.Errorf("failed to delete existing rule: %w", err)
		}
	}

	// Kibana ES query for latency
	esQuery := fmt.Sprintf(`{"query":{"range":{"transaction.duration.us":{"gt":%d}}}}`, latencyThresholdMs*1000)

	// Request body
	reqBody := map[string]interface{}{
		"name":         uniqueRuleName(serviceName),
		"tags":         []string{"latency", serviceName},
		"rule_type_id": ".es-query",
		"consumer":     "alerts",
		"enabled":      true,
		"schedule": map[string]string{
			"interval": "1m",
		},
		"notify_when": "onActionGroupChange",
		"params": map[string]interface{}{
			"index":               []string{"traces-*"},
			"timeField":           "@timestamp",
			"esQuery":             esQuery,
			"size":                1,
			"timeWindowSize":      1,
			"timeWindowUnit":      "m",
			"thresholdComparator": ">",
			"threshold":           []int{0},
		},
		"actions": []map[string]interface{}{
			{
				"id":    c.connectorId,
				"group": "query matched",
				"params": map[string]interface{}{
					"documents": []map[string]interface{}{
						{
							"rule":       uniqueRuleName(serviceName),
							"status":     "firing",
							"service":    serviceName,
							"latency":    "{{context.value}}",
							"@timestamp": "{{context.date}}",
						},
					},
				},
			},
		},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create rule with POST
	url := fmt.Sprintf("%s/api/alerting/rule", c.alertApiUrl)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return "", fmt.Errorf("failed to create POST request: %w", err)
	}
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("kbn-xsrf", "true")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request error: %w", err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("failed to create alert: %s", string(data))
	}

	var respJSON struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &respJSON); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	return respJSON.ID, nil
}

func (c *alertClient) FindRuleByServiceName(serviceName string) (string, error) {
	url := fmt.Sprintf("%s/api/alerting/rules/_find?search=%s&search_fields=name", c.alertApiUrl, url.QueryEscape(uniqueRuleName(serviceName)))

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("kbn-xsrf", "true")
	req.SetBasicAuth(c.username, c.password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to find rule: %s", string(b))
	}

	var result struct {
		Data []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if len(result.Data) == 0 {
		return "", nil // not found
	}

	return result.Data[0].ID, nil
}

func uniqueRuleName(serviceName string) string {
	return fmt.Sprintf("latency-rule-%s", serviceName)
}
