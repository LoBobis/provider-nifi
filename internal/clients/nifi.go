package clients

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/antihax/optional"
	"github.com/pkg/errors"

	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
)

// bearerAuthTransport is an http.RoundTripper that injects a Bearer token
// into the Authorization header of every outgoing request.
type bearerAuthTransport struct {
	token     string
	transport http.RoundTripper
}

func (t *bearerAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+t.token)
	return t.transport.RoundTrip(req)
}

// NiFiConfig holds the configuration for connecting to a NiFi instance.
type NiFiConfig struct {
	// URL is the NiFi API base URL (e.g., https://nifi:8443/nifi-api).
	URL string `json:"url"`

	// Username for basic authentication.
	Username string `json:"username,omitempty"`

	// Password for basic authentication.
	Password string `json:"password,omitempty"`

	// Token is a bearer token for authentication.
	Token string `json:"token,omitempty"`

	// TLSSkipVerify skips TLS certificate verification.
	TLSSkipVerify bool `json:"tlsSkipVerify,omitempty"`
}

// NiFiClient wraps the nigoapi client and provides a unified interface
// for interacting with NiFi 1.x and 2.x clusters.
type NiFiClient struct {
	client *nigoapi.APIClient
	ctx    context.Context
	config NiFiConfig
}

// NewNiFiClient creates a new NiFi API client from raw credential JSON.
func NewNiFiClient(credentialData []byte) (*NiFiClient, error) {
	var cfg NiFiConfig
	if err := json.Unmarshal(credentialData, &cfg); err != nil {
		return nil, errors.Wrap(err, "cannot parse NiFi credentials JSON")
	}

	if cfg.URL == "" {
		return nil, errors.New("NiFi URL is required in credentials")
	}

	return NewNiFiClientFromConfig(cfg)
}

// NewNiFiClientFromConfig creates a new NiFi API client from a config struct.
// If username/password are provided, it performs a token exchange via
// POST /access/token to obtain a JWT, then configures the HTTP client
// to inject Authorization: Bearer <token> on all subsequent requests.
func NewNiFiClientFromConfig(cfg NiFiConfig) (*NiFiClient, error) {
	// Base TLS transport used for all HTTP calls (including token exchange).
	baseTransport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.TLSSkipVerify, //nolint:gosec
		},
	}

	// Resolve the bearer token: either provided directly or obtained via
	// username/password credentials.
	token := cfg.Token
	if token == "" && cfg.Username != "" && cfg.Password != "" {
		var err error
		token, err = fetchAccessToken(cfg.URL, cfg.Username, cfg.Password, baseTransport)
		if err != nil {
			return nil, errors.Wrap(err, "cannot obtain NiFi access token")
		}
	}

	// Choose the transport: if we have a token, wrap with bearer auth;
	// otherwise use the plain TLS transport (e.g. unsecured NiFi).
	var transport http.RoundTripper = baseTransport
	if token != "" {
		transport = &bearerAuthTransport{
			token:     token,
			transport: baseTransport,
		}
	}

	apiCfg := nigoapi.NewConfiguration()
	apiCfg.BasePath = cfg.URL
	apiCfg.HTTPClient = &http.Client{Transport: transport}

	client := nigoapi.NewAPIClient(apiCfg)
	ctx := context.Background()

	return &NiFiClient{
		client: client,
		ctx:    ctx,
		config: cfg,
	}, nil
}

// fetchAccessToken performs a POST to /access/token with URL-encoded
// username/password credentials and returns the raw JWT string.
func fetchAccessToken(baseURL, username, password string, transport http.RoundTripper) (string, error) {
	tokenURL := strings.TrimSuffix(baseURL, "/") + "/access/token"

	form := url.Values{}
	form.Set("username", username)
	form.Set("password", password)

	req, err := http.NewRequest(http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.Wrap(err, "cannot build token request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	httpClient := &http.Client{Transport: transport}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "token request failed")
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "cannot read token response body")
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("token request returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	token := strings.TrimSpace(string(body))
	if token == "" {
		return "", errors.New("NiFi returned an empty access token")
	}

	return token, nil
}

// Context returns the authentication context.
func (c *NiFiClient) Context() context.Context {
	return c.ctx
}

// --- Processor Operations ---

// GetProcessor retrieves a processor by ID.
func (c *NiFiClient) GetProcessor(id string) (*nigoapi.ProcessorEntity, error) {
	entity, resp, _, err := c.client.ProcessorsApi.GetProcessor(c.ctx, id)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "get processor %s", id)
	}
	return &entity, nil
}

// CreateProcessor creates a new processor in the given process group.
func (c *NiFiClient) CreateProcessor(parentGroupID string, entity nigoapi.ProcessorEntity) (*nigoapi.ProcessorEntity, error) {
	result, resp, _, err := c.client.ProcessGroupsApi.CreateProcessor(c.ctx, entity, parentGroupID)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "create processor in group %s", parentGroupID)
	}
	return &result, nil
}

// UpdateProcessor updates an existing processor.
func (c *NiFiClient) UpdateProcessor(entity nigoapi.ProcessorEntity) (*nigoapi.ProcessorEntity, error) {
	result, resp, _, err := c.client.ProcessorsApi.UpdateProcessor(c.ctx, entity, entity.Id)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "update processor %s", entity.Id)
	}
	return &result, nil
}

// DeleteProcessor deletes a processor by ID.
func (c *NiFiClient) DeleteProcessor(id string, version int64) error {
	_, resp, _, err := c.client.ProcessorsApi.DeleteProcessor(c.ctx, id, &nigoapi.ProcessorsApiDeleteProcessorOpts{
		Version: optionalStringFromInt64(version),
	})
	if err != nil {
		return wrapNiFiError(err, resp, "delete processor %s", id)
	}
	return nil
}

// UpdateProcessorRunStatus updates the run status of a processor.
func (c *NiFiClient) UpdateProcessorRunStatus(id string, state string, version int64) error {
	entity := nigoapi.ProcessorRunStatusEntity{
		State: state,
		Revision: &nigoapi.RevisionDto{
			Version: &version,
		},
	}
	_, resp, _, err := c.client.ProcessorsApi.UpdateRunStatus4(c.ctx, entity, id)
	if err != nil {
		return wrapNiFiError(err, resp, "update processor %s run status to %s", id, state)
	}
	return nil
}

// --- Process Group Operations ---

// GetProcessGroup retrieves a process group by ID.
func (c *NiFiClient) GetProcessGroup(id string) (*nigoapi.ProcessGroupEntity, error) {
	entity, resp, _, err := c.client.ProcessGroupsApi.GetProcessGroup(c.ctx, id)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "get process group %s", id)
	}
	return &entity, nil
}

// CreateProcessGroup creates a new process group.
func (c *NiFiClient) CreateProcessGroup(parentGroupID string, entity nigoapi.ProcessGroupEntity) (*nigoapi.ProcessGroupEntity, error) {
	result, resp, _, err := c.client.ProcessGroupsApi.CreateProcessGroup(c.ctx, entity, parentGroupID, nil)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "create process group in %s", parentGroupID)
	}
	return &result, nil
}

// UpdateProcessGroup updates an existing process group.
func (c *NiFiClient) UpdateProcessGroup(entity nigoapi.ProcessGroupEntity) (*nigoapi.ProcessGroupEntity, error) {
	result, resp, _, err := c.client.ProcessGroupsApi.UpdateProcessGroup(c.ctx, entity, entity.Id)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "update process group %s", entity.Id)
	}
	return &result, nil
}

// DeleteProcessGroup deletes a process group by ID.
func (c *NiFiClient) DeleteProcessGroup(id string, version int64) error {
	_, resp, _, err := c.client.ProcessGroupsApi.RemoveProcessGroup(c.ctx, id, &nigoapi.ProcessGroupsApiRemoveProcessGroupOpts{
		Version: optionalStringFromInt64(version),
	})
	if err != nil {
		return wrapNiFiError(err, resp, "delete process group %s", id)
	}
	return nil
}

// ScheduleProcessGroup starts or stops all processors in a process group.
func (c *NiFiClient) ScheduleProcessGroup(id string, state string) error {
	entity := nigoapi.ScheduleComponentsEntity{
		Id:    id,
		State: state,
	}
	_, resp, _, err := c.client.FlowApi.ScheduleComponents(c.ctx, entity, id)
	if err != nil {
		return wrapNiFiError(err, resp, "schedule process group %s to %s", id, state)
	}
	return nil
}

// --- Connection Operations ---

// GetConnection retrieves a connection by ID.
func (c *NiFiClient) GetConnection(id string) (*nigoapi.ConnectionEntity, error) {
	entity, resp, _, err := c.client.ConnectionsApi.GetConnection(c.ctx, id)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "get connection %s", id)
	}
	return &entity, nil
}

// CreateConnection creates a new connection.
func (c *NiFiClient) CreateConnection(parentGroupID string, entity nigoapi.ConnectionEntity) (*nigoapi.ConnectionEntity, error) {
	result, resp, _, err := c.client.ProcessGroupsApi.CreateConnection(c.ctx, entity, parentGroupID)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "create connection in group %s", parentGroupID)
	}
	return &result, nil
}

// UpdateConnection updates an existing connection.
func (c *NiFiClient) UpdateConnection(entity nigoapi.ConnectionEntity) (*nigoapi.ConnectionEntity, error) {
	result, resp, _, err := c.client.ConnectionsApi.UpdateConnection(c.ctx, entity, entity.Id)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "update connection %s", entity.Id)
	}
	return &result, nil
}

// DeleteConnection deletes a connection by ID.
func (c *NiFiClient) DeleteConnection(id string, version int64) error {
	_, resp, _, err := c.client.ConnectionsApi.DeleteConnection(c.ctx, id, &nigoapi.ConnectionsApiDeleteConnectionOpts{
		Version: optionalStringFromInt64(version),
	})
	if err != nil {
		return wrapNiFiError(err, resp, "delete connection %s", id)
	}
	return nil
}

// --- Controller Service Operations ---

// GetControllerService retrieves a controller service by ID.
func (c *NiFiClient) GetControllerService(id string) (*nigoapi.ControllerServiceEntity, error) {
	entity, resp, _, err := c.client.ControllerServicesApi.GetControllerService(c.ctx, id, nil)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "get controller service %s", id)
	}
	return &entity, nil
}

// CreateControllerService creates a new controller service.
func (c *NiFiClient) CreateControllerService(parentGroupID string, entity nigoapi.ControllerServiceEntity) (*nigoapi.ControllerServiceEntity, error) {
	result, resp, _, err := c.client.ProcessGroupsApi.CreateControllerService1(c.ctx, entity, parentGroupID)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "create controller service in group %s", parentGroupID)
	}
	return &result, nil
}

// UpdateControllerService updates an existing controller service.
func (c *NiFiClient) UpdateControllerService(entity nigoapi.ControllerServiceEntity) (*nigoapi.ControllerServiceEntity, error) {
	result, resp, _, err := c.client.ControllerServicesApi.UpdateControllerService(c.ctx, entity, entity.Id)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "update controller service %s", entity.Id)
	}
	return &result, nil
}

// DeleteControllerService deletes a controller service by ID.
func (c *NiFiClient) DeleteControllerService(id string, version int64) error {
	_, resp, _, err := c.client.ControllerServicesApi.RemoveControllerService(c.ctx, id, &nigoapi.ControllerServicesApiRemoveControllerServiceOpts{
		Version: optionalStringFromInt64(version),
	})
	if err != nil {
		return wrapNiFiError(err, resp, "delete controller service %s", id)
	}
	return nil
}

// UpdateControllerServiceRunStatus enables or disables a controller service.
func (c *NiFiClient) UpdateControllerServiceRunStatus(id string, state string, version int64) error {
	entity := nigoapi.ControllerServiceRunStatusEntity{
		State: state,
		Revision: &nigoapi.RevisionDto{
			Version: &version,
		},
	}
	_, resp, _, err := c.client.ControllerServicesApi.UpdateRunStatus1(c.ctx, entity, id)
	if err != nil {
		return wrapNiFiError(err, resp, "update controller service %s state to %s", id, state)
	}
	return nil
}

// --- Parameter Context Operations ---

// GetParameterContext retrieves a parameter context by ID.
func (c *NiFiClient) GetParameterContext(id string) (*nigoapi.ParameterContextEntity, error) {
	entity, resp, _, err := c.client.ParameterContextsApi.GetParameterContext(c.ctx, id, &nigoapi.ParameterContextsApiGetParameterContextOpts{})
	if err != nil {
		return nil, wrapNiFiError(err, resp, "get parameter context %s", id)
	}
	return &entity, nil
}

// CreateParameterContext creates a new parameter context.
func (c *NiFiClient) CreateParameterContext(entity nigoapi.ParameterContextEntity) (*nigoapi.ParameterContextEntity, error) {
	result, resp, _, err := c.client.ParameterContextsApi.CreateParameterContext(c.ctx, entity)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "create parameter context")
	}
	return &result, nil
}

// UpdateParameterContext updates an existing parameter context.
func (c *NiFiClient) UpdateParameterContext(entity nigoapi.ParameterContextEntity) (*nigoapi.ParameterContextEntity, error) {
	result, resp, _, err := c.client.ParameterContextsApi.UpdateParameterContext(c.ctx, entity, entity.Id)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "update parameter context %s", entity.Id)
	}
	return &result, nil
}

// DeleteParameterContext deletes a parameter context by ID.
func (c *NiFiClient) DeleteParameterContext(id string, version int64) error {
	_, resp, _, err := c.client.ParameterContextsApi.DeleteParameterContext(c.ctx, id, &nigoapi.ParameterContextsApiDeleteParameterContextOpts{
		Version: optionalStringFromInt64(version),
	})
	if err != nil {
		return wrapNiFiError(err, resp, "delete parameter context %s", id)
	}
	return nil
}

// --- Registry / Version Control Operations ---

// ImportFlowFromRegistry creates a versioned process group from a NiFi Registry flow.
func (c *NiFiClient) ImportFlowFromRegistry(parentGroupID string, registryID, bucketID, flowID string, flowVersion int32, position *nigoapi.PositionDto) (*nigoapi.ProcessGroupEntity, error) {
	var version interface{}
	if flowVersion <= 0 {
		version = int32(-1)
	} else {
		version = flowVersion
	}

	var initialVersion int64
	entity := nigoapi.ProcessGroupEntity{
		Component: &nigoapi.ProcessGroupDto{
			Position: position,
			VersionControlInformation: &nigoapi.VersionControlInformationDto{
				RegistryId: registryID,
				BucketId:   bucketID,
				FlowId:     flowID,
				Version:    version,
			},
		},
		Revision: &nigoapi.RevisionDto{
			Version: &initialVersion,
		},
	}

	result, resp, _, err := c.client.ProcessGroupsApi.CreateProcessGroup(c.ctx, entity, parentGroupID, nil)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "import flow from registry to group %s", parentGroupID)
	}
	return &result, nil
}

// GetVersionControlInfo retrieves version control information for a process group.
func (c *NiFiClient) GetVersionControlInfo(processGroupID string) (*nigoapi.VersionControlInformationEntity, error) {
	entity, resp, _, err := c.client.VersionsApi.GetVersionInformation(c.ctx, processGroupID)
	if err != nil {
		return nil, wrapNiFiError(err, resp, "get version control info for %s", processGroupID)
	}
	return &entity, nil
}

// ChangeFlowVersion initiates a version control update to change the deployed flow version.
func (c *NiFiClient) ChangeFlowVersion(processGroupID string, vci nigoapi.VersionControlInformationEntity) error {
	_, resp, _, err := c.client.VersionsApi.InitiateVersionControlUpdate(c.ctx, vci, processGroupID)
	if err != nil {
		return wrapNiFiError(err, resp, "change flow version for %s", processGroupID)
	}
	return nil
}

// --- Utility Functions ---

// NotFoundError is returned when a NiFi resource is not found (HTTP 404).
type NotFoundError struct {
	msg string
}

func (e *NotFoundError) Error() string {
	return e.msg
}

// IsNotFound returns true if the error indicates a NiFi resource was not found.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nfe *NotFoundError
	return errors.As(err, &nfe)
}

// wrapNiFiError wraps a NiFi API error with context.
// If the HTTP response indicates a 404, it returns a NotFoundError.
// nigoapi often returns nil for *http.Response on errors, so we also
// detect 404 from the GenericSwaggerError string (e.g. "404 Not Found").
func wrapNiFiError(err error, resp *http.Response, format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)

	// Check HTTP response status code first.
	if resp != nil && resp.StatusCode == http.StatusNotFound {
		return &NotFoundError{msg: fmt.Sprintf("NiFi resource not found: %s", msg)}
	}

	// When resp is nil, detect 404 from the swagger error string.
	// GenericSwaggerError.Error() returns the HTTP status like "404 Not Found".
	if swaggerErr, ok := err.(nigoapi.GenericSwaggerError); ok {
		if strings.Contains(swaggerErr.Error(), "404") {
			return &NotFoundError{msg: fmt.Sprintf("NiFi resource not found: %s", msg)}
		}
		return errors.Wrapf(err, "NiFi API error: %s (body: %s)", msg, string(swaggerErr.Body()))
	}

	// Fallback: check the raw error string for 404 indicators.
	if strings.Contains(err.Error(), "404") {
		return &NotFoundError{msg: fmt.Sprintf("NiFi resource not found: %s", msg)}
	}

	return errors.Wrapf(err, "NiFi API error: %s", msg)
}

// optionalStringFromInt64 converts an int64 to an optional.Interface for API calls.
func optionalStringFromInt64(v int64) optional.Interface {
	return optional.NewInterface(fmt.Sprintf("%d", v))
}
