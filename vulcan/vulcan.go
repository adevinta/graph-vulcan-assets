// Package vulcan allows to consume the [Vulcan async API].
//
// [Vulcan async API]: https://github.com/adevinta/vulcan-api/blob/master/docs/asyncapi.yaml
package vulcan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/adevinta/graph-vulcan-assets/stream"
)

// vulcanMajorVersion is the major version of the Vulcan asynchronous API
// supported by [Client].
const vulcanMajorVersion = 0

// AssetsEntityName is the name of the entity linked to assets.
const assetsEntityName = "assets-v0"

// AssetPayload represents the "assetPayload" model as defined by the Vulcan
// async API.
type AssetPayload struct {
	Id          string
	Team        Team
	Alias       string
	Rolfp       string
	Scannable   bool
	AssetType   AssetType
	Identifier  string
	Annotations []Annotation
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

// AssetMetadata represents the "assetMetadata" model as defined by the Vulcan
// async API.
type AssetMetadata struct {
	Version    string
	Type       AssetType
	Identifier string
}

// Client is a Vulcan async API client.
type Client struct {
	proc stream.Processor
}

// AssetHandler processes an asset. isNil is true when the value of the stream
// message is nil.
type AssetHandler func(id string, payload AssetPayload, isNil bool) error

// NewClient returns a client for the Vulcan async API using the provided
// stream processor.
func NewClient(proc stream.Processor) Client {
	return Client{proc}
}

// ProcessAssets receives assets from the underlying stream and processes them
// using the provided handler. This method blocks the calling goroutine until
// the specified context is cancelled.
func (c Client) ProcessAssets(ctx context.Context, h AssetHandler) error {
	return c.proc.Process(ctx, assetsEntityName, func(msg stream.Message) error {
		version, typ, identifier, err := parseMetadata(msg)
		if err != nil {
			return fmt.Errorf("invalid metadata: %v", err)
		}

		if !supportedVersion(version) {
			// Do not process asset on version mismatch.
			return nil
		}

		id := string(msg.Key)

		var (
			payload AssetPayload
			isNil   bool
		)

		if msg.Value != nil {
			if err := json.Unmarshal(msg.Value, &payload); err != nil {
				return fmt.Errorf("could not unmarshal asset with ID %v: %w", id, err)
			}
		} else {
			payload.AssetType = AssetType(typ)
			payload.Identifier = identifier
			isNil = true
		}

		return h(id, payload, isNil)
	})
}

// parseMetadata parses and validates message metadata.
func parseMetadata(msg stream.Message) (version, typ, identifier string, err error) {
	for _, e := range msg.Metadata {
		key := string(e.Key)
		value := string(e.Value)

		switch key {
		case "version":
			version = value
		case "type":
			typ = value
		case "identifier":
			identifier = value
		}
	}

	if version == "" || typ == "" || identifier == "" {
		return "", "", "", errors.New("missing metadata entry")
	}

	return version, typ, identifier, nil
}

// supportedVersion takes a semantic version string and returns true if it is
// compatible with [Client].
func supportedVersion(v string) bool {
	if v == "" {
		return false
	}

	if v[0] == 'v' {
		v = v[1:]
	}

	parts := strings.Split(v, ".")
	if len(parts) < 3 {
		return false
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}

	return major == vulcanMajorVersion
}
