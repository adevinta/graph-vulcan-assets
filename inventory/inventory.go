// Package inventory allows to interact with the [Graph Asset Inventory REST
// API].
//
// [Graph Asset Inventory REST API]: https://github.com/adevinta/graph-asset-inventory-api/blob/master/graph_asset_inventory_api/openapi/graph-asset-inventory-api.yaml
package inventory

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"
)

var (
	// ErrNotFound is returned when an entity cannot be found in the Asset
	// Inventory.
	ErrNotFound = errors.New("not found")

	// ErrAlreadyExists is returned when trying to create an entity that
	// already exists.
	ErrAlreadyExists = errors.New("already exists")

	// Unexpired is the [time.Time] expiration assigned to unexpired
	// entities.
	Unexpired time.Time = *strtime("9999-12-12T23:59:59Z")
)

// InvalidStatusError is returned when a call to an endpoint of the Graph Asset
// Inventory did not return the expected status code.
type InvalidStatusError struct {
	Expected []int
	Returned int
}

func (w InvalidStatusError) Error() string {
	return fmt.Sprintf("invalid status response code %v, expected %v", w.Returned, w.Expected)
}

// TeamReq represents the "TeamReq" model as defined by the Graph Asset
// Inventory REST API.
type TeamReq struct {
	Identifier string `json:"identifier"`
	Name       string `json:"name"`
}

// TeamResp represents the "TeamResp" model as defined by the Graph Asset
// Inventory REST API.
type TeamResp struct {
	ID         string `json:"id"`
	Identifier string `json:"identifier"`
	Name       string `json:"name"`
}

// AssetReq represents the "AssetReq" model as defined by the Graph Asset
// Inventory REST API.
type AssetReq struct {
	Type       string     `json:"type"`
	Identifier string     `json:"identifier"`
	Timestamp  *time.Time `json:"timestamp,omitempty"`
	Expiration time.Time  `json:"expiration"`
}

// AssetResp represents the "AssetResp" model as defined by the Graph Asset
// Inventory REST API.
type AssetResp struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	Identifier string    `json:"identifier"`
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
	Expiration time.Time `json:"expiration"`
}

// ParentOfReq represents the "ParentOfReq" model as defined by the Graph Asset
// Inventory REST API.
type ParentOfReq struct {
	Timestamp  *time.Time `json:"timestamp,omitempty"`
	Expiration time.Time  `json:"expiration"`
}

// ParentOfResp represents the "ParentOfResp" model as defined by the Graph Asset
// Inventory REST API.
type ParentOfResp struct {
	ID         string    `json:"id"`
	ParentID   string    `json:"parent_id"`
	ChildID    string    `json:"child_id"`
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
	Expiration time.Time `json:"expiration"`
}

// OwnsReq represents the "OwnsReq" model as defined by the Graph Asset
// Inventory REST API.
type OwnsReq struct {
	StartTime time.Time  `json:"start_time"`
	EndTime   *time.Time `json:"end_time,omitempty"`
}

// OwnsResp represents the "OwnsResp" model as defined by the Graph Asset
// Inventory REST API.
type OwnsResp struct {
	ID        string     `json:"id"`
	TeamID    string     `json:"team_id"`
	AssetID   string     `json:"asset_id"`
	StartTime time.Time  `json:"start_time"`
	EndTime   *time.Time `json:"end_time,omitempty"`
}

// Pagination contains the pagination parameters. If the Size field is zero,
// pagination is disabled.
type Pagination struct {
	Page int
	Size int
}

// Client represents a client of the Graph Asset Inventory REST API.
type Client struct {
	endpoint *url.URL
	httpcli  http.Client
}

// NewClient returns a [Client] pointing to the given endpoint (for instance
// https://security-graph-asset-inventory/), and optionally skipping the
// verification of the endpoint server certificate.
func NewClient(endpoint string, insecureSkipVerify bool) (Client, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: insecureSkipVerify},
	}
	httpcli := http.Client{Transport: tr}

	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return Client{}, fmt.Errorf("invalid endpoint %s", endpoint)
	}

	cli := Client{
		endpoint: endpointURL,
		httpcli:  httpcli,
	}
	return cli, nil
}

func (cli Client) urlTeams(identifier string, pag Pagination) string {
	u := cli.endpoint.JoinPath("/v1/teams")

	q := u.Query()
	if identifier != "" {
		q.Set("team_identifier", identifier)
	}
	if pag.Size != 0 {
		q.Set("page", strconv.Itoa(pag.Page))
		q.Set("size", strconv.Itoa(pag.Size))
	}
	u.RawQuery = q.Encode()

	return u.String()
}

func (cli Client) urlAssets(typ, identifier string, validAt time.Time, pag Pagination) string {
	u := cli.endpoint.JoinPath("/v1/assets")

	q := u.Query()
	if typ != "" {
		q.Set("asset_type", typ)
	}
	if identifier != "" {
		q.Set("asset_identifier", identifier)
	}
	if !validAt.IsZero() {
		q.Set("valid_at", validAt.Format(time.RFC3339))
	}
	if pag.Size != 0 {
		q.Set("page", strconv.Itoa(pag.Page))
		q.Set("size", strconv.Itoa(pag.Size))
	}
	u.RawQuery = q.Encode()

	return u.String()
}

func (cli Client) urlAssetsID(id string) string {
	p := "/v1/assets"
	p = path.Join(p, id)
	u := cli.endpoint.JoinPath(p)

	return u.String()
}

func (cli Client) urlOwners(assetID string, pag Pagination) string {
	p := "/v1/assets"
	p = path.Join(p, assetID)
	p = path.Join(p, "owners")
	u := cli.endpoint.JoinPath(p)

	q := u.Query()
	if pag.Size != 0 {
		q.Set("page", strconv.Itoa(pag.Page))
		q.Set("size", strconv.Itoa(pag.Size))
	}
	u.RawQuery = q.Encode()

	return u.String()
}

func (cli Client) urlOwnersID(assetID, teamID string) string {
	p := "/v1/assets"
	p = path.Join(p, assetID)
	p = path.Join(p, "owners")
	p = path.Join(p, teamID)
	u := cli.endpoint.JoinPath(p)

	return u.String()
}

func (cli Client) urlParents(id string, pag Pagination) string {
	p := "/v1/assets"
	p = path.Join(p, id)
	p = path.Join(p, "parents")
	u := cli.endpoint.JoinPath(p)

	q := u.Query()
	if pag.Size != 0 {
		q.Set("page", strconv.Itoa(pag.Page))
		q.Set("size", strconv.Itoa(pag.Size))
	}
	u.RawQuery = q.Encode()

	return u.String()
}

func (cli Client) urlParentsID(childID, parentID string) string {
	p := "/v1/assets"
	p = path.Join(p, childID)
	p = path.Join(p, "parents")
	p = path.Join(p, parentID)
	u := cli.endpoint.JoinPath(p)

	return u.String()
}

// Teams returns a list of teams filtered by identifier. If identifier is
// empty, no filter is applied. The pag parameter controls pagination.
func (cli Client) Teams(identifier string, pag Pagination) ([]TeamResp, error) {
	u := cli.urlTeams(identifier, pag)
	resp, err := cli.httpcli.Get(u)
	if err != nil {
		return nil, fmt.Errorf("HTTP request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := InvalidStatusError{
			Expected: []int{http.StatusOK},
			Returned: resp.StatusCode,
		}
		return nil, err
	}

	var teams []TeamResp
	if err := json.NewDecoder(resp.Body).Decode(&teams); err != nil {
		return nil, fmt.Errorf("invalid response: %w", err)
	}

	return teams, nil
}

// CreateTeam creates a team with the given identifier and name. It returns the
// the created team.
func (cli Client) CreateTeam(identifier, name string) (TeamResp, error) {
	var data bytes.Buffer
	payload := TeamReq{
		Identifier: identifier,
		Name:       name,
	}
	if err := json.NewEncoder(&data).Encode(payload); err != nil {
		return TeamResp{}, fmt.Errorf("invalid payload: %w", err)
	}

	u := cli.urlTeams("", Pagination{})
	resp, err := cli.httpcli.Post(u, "application/json", &data)
	if err != nil {
		return TeamResp{}, fmt.Errorf("HTTP request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		if resp.StatusCode == http.StatusConflict {
			return TeamResp{}, ErrAlreadyExists
		}
		err := InvalidStatusError{
			Expected: []int{http.StatusCreated},
			Returned: resp.StatusCode,
		}
		return TeamResp{}, err
	}

	var team TeamResp
	if err := json.NewDecoder(resp.Body).Decode(&team); err != nil {
		return TeamResp{}, fmt.Errorf("invalid response: %w", err)
	}

	return team, nil
}

// Assets returns a list of assets filtered by type and identifier. If typ,
// identifier are empty and validAt is zero, no filter is applied. The pag
// parameter controls pagination.
func (cli Client) Assets(typ, identifier string, validAt time.Time, pag Pagination) ([]AssetResp, error) {
	u := cli.urlAssets(typ, identifier, validAt, pag)
	resp, err := cli.httpcli.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := InvalidStatusError{
			Expected: []int{http.StatusOK},
			Returned: resp.StatusCode,
		}
		return nil, err
	}

	var assets []AssetResp
	if err := json.NewDecoder(resp.Body).Decode(&assets); err != nil {
		return nil, fmt.Errorf("invalid response: %w", err)
	}

	return assets, nil

}

// CreateAsset creates an asset with the given type, identifier and expiration.
// It returns the the created asset.
func (cli Client) CreateAsset(typ, identifier string, timestamp, expiration time.Time) (AssetResp, error) {
	var data bytes.Buffer
	payload := AssetReq{
		Type:       typ,
		Identifier: identifier,
		Expiration: expiration,
	}
	if !timestamp.IsZero() {
		payload.Timestamp = &timestamp
	}
	if err := json.NewEncoder(&data).Encode(payload); err != nil {
		return AssetResp{}, fmt.Errorf("invalid payload: %w", err)
	}

	u := cli.urlAssets("", "", time.Time{}, Pagination{})
	resp, err := cli.httpcli.Post(u, "application/json", &data)
	if err != nil {
		return AssetResp{}, fmt.Errorf("HTTP request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		if resp.StatusCode == http.StatusConflict {
			return AssetResp{}, ErrAlreadyExists
		}
		err := InvalidStatusError{
			Expected: []int{http.StatusCreated},
			Returned: resp.StatusCode,
		}
		return AssetResp{}, err
	}

	var asset AssetResp
	if err := json.NewDecoder(resp.Body).Decode(&asset); err != nil {
		return AssetResp{}, fmt.Errorf("invalid response: %w", err)
	}

	return asset, nil
}

// UpdateAsset updates an asset with a given ID. The type and the identifier
// must match the asset ID. This method will only update the time attributes of
// the asset if the corresponding parameter is not zero.
func (cli Client) UpdateAsset(id, typ, identifier string, timestamp, expiration time.Time) (AssetResp, error) {
	payload := AssetReq{
		Type:       typ,
		Identifier: identifier,
		Expiration: expiration,
	}
	if !timestamp.IsZero() {
		payload.Timestamp = &timestamp
	}

	var data bytes.Buffer
	if err := json.NewEncoder(&data).Encode(payload); err != nil {
		return AssetResp{}, err
	}

	u := cli.urlAssetsID(id)
	req, err := http.NewRequest(http.MethodPut, u, &data)
	if err != nil {
		return AssetResp{}, fmt.Errorf("could not create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cli.httpcli.Do(req)
	if err != nil {
		return AssetResp{}, fmt.Errorf("HTTP request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return AssetResp{}, ErrNotFound
		}
		err := InvalidStatusError{
			Expected: []int{http.StatusOK},
			Returned: resp.StatusCode,
		}
		return AssetResp{}, err
	}

	var asset AssetResp
	if err := json.NewDecoder(resp.Body).Decode(&asset); err != nil {
		return AssetResp{}, fmt.Errorf("invalid response: %w", err)
	}

	return asset, nil
}

// Parents returns the "parent of" relations of the asset with the given ID.
// The pag parameter controls pagination.
func (cli Client) Parents(assetID string, pag Pagination) ([]ParentOfResp, error) {
	u := cli.urlParents(assetID, pag)
	resp, err := cli.httpcli.Get(u)
	if err != nil {
		return nil, fmt.Errorf("HTTP request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotFound
		}
		err := InvalidStatusError{
			Expected: []int{http.StatusOK},
			Returned: resp.StatusCode,
		}
		return nil, err
	}

	var parents []ParentOfResp
	if err := json.NewDecoder(resp.Body).Decode(&parents); err != nil {
		return nil, fmt.Errorf("invalid response: %w", err)
	}

	return parents, nil
}

// UpsertParent creates or updates the "parent of" relation between the
// provided assets. If timestamp is zero, it is ignored.
func (cli Client) UpsertParent(childID, parentID string, timestamp, expiration time.Time) (ParentOfResp, error) {
	payload := ParentOfReq{
		Expiration: expiration,
	}
	if !timestamp.IsZero() {
		payload.Timestamp = &timestamp
	}

	var data bytes.Buffer
	if err := json.NewEncoder(&data).Encode(payload); err != nil {
		return ParentOfResp{}, err
	}

	u := cli.urlParentsID(childID, parentID)
	req, err := http.NewRequest(http.MethodPut, u, &data)
	if err != nil {
		return ParentOfResp{}, fmt.Errorf("could not create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cli.httpcli.Do(req)
	if err != nil {
		return ParentOfResp{}, fmt.Errorf("HTTP request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		if resp.StatusCode == http.StatusNotFound {
			return ParentOfResp{}, ErrNotFound
		}
		err := InvalidStatusError{
			Expected: []int{http.StatusOK, http.StatusCreated},
			Returned: resp.StatusCode,
		}
		return ParentOfResp{}, err
	}

	var parents ParentOfResp
	if err := json.NewDecoder(resp.Body).Decode(&parents); err != nil {
		return ParentOfResp{}, fmt.Errorf("invalid response: %w", err)
	}

	return parents, nil
}

// Owners returns the "owns" relations of the asset with the provided ID. The
// pag parameter controls pagination.
func (cli Client) Owners(assetID string, pag Pagination) ([]OwnsResp, error) {
	u := cli.urlOwners(assetID, pag)
	resp, err := cli.httpcli.Get(u)
	if err != nil {
		return nil, fmt.Errorf("HTTP request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotFound
		}
		err := InvalidStatusError{
			Expected: []int{http.StatusOK},
			Returned: resp.StatusCode,
		}
		return nil, err
	}

	var owners []OwnsResp
	if err := json.NewDecoder(resp.Body).Decode(&owners); err != nil {
		return nil, fmt.Errorf("invalid response: %w", err)
	}

	return owners, nil
}

// UpsertOwner creates or updates the "owns" relation between the provided team
// and asset. If endTime is zero, it is ignored.
func (cli Client) UpsertOwner(assetID, teamID string, startTime, endTime time.Time) (OwnsResp, error) {
	payload := OwnsReq{
		StartTime: startTime,
	}
	if !endTime.IsZero() {
		payload.EndTime = &endTime
	}

	var data bytes.Buffer
	if err := json.NewEncoder(&data).Encode(payload); err != nil {
		return OwnsResp{}, err
	}

	u := cli.urlOwnersID(assetID, teamID)
	req, err := http.NewRequest(http.MethodPut, u, &data)
	if err != nil {
		return OwnsResp{}, fmt.Errorf("could not create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cli.httpcli.Do(req)
	if err != nil {
		return OwnsResp{}, fmt.Errorf("HTTP request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		if resp.StatusCode == http.StatusNotFound {
			return OwnsResp{}, ErrNotFound
		}
		err := InvalidStatusError{
			Expected: []int{http.StatusOK, http.StatusCreated},
			Returned: resp.StatusCode,
		}
		return OwnsResp{}, err
	}

	var owner OwnsResp
	if err := json.NewDecoder(resp.Body).Decode(&owner); err != nil {
		return OwnsResp{}, fmt.Errorf("invalid response: %w", err)
	}

	return owner, nil
}

// strtime takes a time string with layout RFC3339 and returns the parsed
// [time.Time]. It panics on error and is meant to be used on variable
// initialization.
func strtime(s string) *time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return &t
}
