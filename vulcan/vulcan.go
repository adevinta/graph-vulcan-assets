// Package vulcan allows to consume the [Vulcan async API].
//
// [Vulcan async API]: https://github.com/adevinta/vulcan-api/blob/master/docs/asyncapi.yaml
package vulcan

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/adevinta/graph-vulcan-assets/stream"
)

// AssetsEntityName is the name of the entity linked to assets.
const assetsEntityName = "assets"

// AssetPayload represents the "assetPayload" model as defined by the Vulcan
// async API.
type AssetPayload struct {
	Id          string
	Team        *Team
	Alias       string
	Rolfp       string
	Scannable   bool
	AssetType   *AssetType
	Identifier  string
	Annotations []*Annotation
}

// Team represents the "team" model as defined by the Vulcan async API.
type Team struct {
	Id          string
	Name        string
	Description string
	Tag         string
}

// AssetType represents the "assetType" model as defined by the Vulcan async
// API.
type AssetType string

// Annotation represents the "annotation" model as defined by the Vulcan async
// API.
type Annotation struct {
	Key   string
	Value string
}

// Client is a Vulcan async API client.
type Client struct {
	proc stream.Processor
}

// AssetHandler processes an asset.
type AssetHandler func(id string, payload AssetPayload) error

// NewClient returns a client for the Vulcan async API using the provided
// stream processor.
func NewClient(proc stream.Processor) Client {
	return Client{proc}
}

// ProcessAssets receives assets from the underlying stream and processes them
// using the provided handler. This method blocks the calling goroutine until
// the specified context is cancelled.
func (c Client) ProcessAssets(ctx context.Context, h AssetHandler) error {
	return c.proc.Process(ctx, assetsEntityName, func(key []byte, value []byte) error {
		id := string(key)

		var payload AssetPayload
		if err := json.Unmarshal(value, &payload); err != nil {
			return fmt.Errorf("could not unmarshal asset with ID %v: %w", id, err)
		}

		return h(id, payload)
	})
}
