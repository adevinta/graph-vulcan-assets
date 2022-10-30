package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/adevinta/graph-vulcan-assets/inventory"
	"github.com/adevinta/graph-vulcan-assets/stream"
	"github.com/adevinta/graph-vulcan-assets/stream/streamtest"
	"github.com/adevinta/graph-vulcan-assets/vulcan"
)

const (
	bootstrapServers = "127.0.0.1:9092"
	gremlinEndpoint  = "ws://127.0.0.1:8182/gremlin"
	messagesFile     = "testdata/messages.json"
	timeout          = 5 * time.Minute
)

func setupKafka() error {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,

		// Set message timeout to 5s, so the kafka client returns an
		// error if the broker is not up.
		"message.timeout.ms": 5000,
	}

	prod, err := kafka.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("error creating producer: %v", err)
	}
	defer prod.Close()

	admin, err := kafka.NewAdminClientFromProducer(prod)
	if err != nil {
		return fmt.Errorf("error creating admin client: %v", err)
	}

	adminOpts := []kafka.DeleteTopicsAdminOption{
		kafka.SetAdminRequestTimeout(timeout),
		kafka.SetAdminOperationTimeout(timeout),
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if _, err := admin.DeleteTopics(ctx, []string{vulcan.AssetsEntityName}, adminOpts...); err != nil {
		return fmt.Errorf("error deleting topic: %v", err)
	}

	msgs := streamtest.Parse(messagesFile)
	for _, msg := range msgs {
		if err := produceMessage(prod, vulcan.AssetsEntityName, msg); err != nil {
			return fmt.Errorf("error producing message: %v", err)
		}
	}

	for prod.Flush(10000) > 0 {
		// Waiting to flush outstanding messages.
	}

	return nil
}

func produceMessage(prod *kafka.Producer, topic string, msg stream.Message) error {
	events := make(chan kafka.Event)
	defer close(events)

	kmsg := &kafka.Message{
		Key:            msg.Key,
		Value:          msg.Value,
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	}

	for _, e := range msg.Metadata {
		hdr := kafka.Header{
			Key:   string(e.Key),
			Value: e.Value,
		}
		kmsg.Headers = append(kmsg.Headers, hdr)
	}

	if err := prod.Produce(kmsg, events); err != nil {
		return fmt.Errorf("failed to produce message: %v", err)
	}

	e := <-events
	kmsg, ok := e.(*kafka.Message)
	if !ok {
		return errors.New("event type is not *kafka.Message")
	}
	if kmsg.TopicPartition.Error != nil {
		return fmt.Errorf("could not deliver message: %w", kmsg.TopicPartition.Error)
	}

	return nil
}

func resetInventory() error {
	conn, err := gremlingo.NewDriverRemoteConnection(gremlinEndpoint, func(settings *gremlingo.DriverRemoteConnectionSettings) {
		settings.LogVerbosity = gremlingo.Off
	})
	if err != nil {
		return fmt.Errorf("could not connect to Neptune: %v", err)
	}
	defer conn.Close()

	g := gremlingo.Traversal_().WithRemote(conn)

	g.V().Not(gremlingo.T__.HasLabel("Universe")).Drop().Next()

	return nil
}

type (
	testdata struct {
		Teams  []tdTeam
		Assets []tdAsset
	}

	tdTeam struct {
		Identifier string
		Name       string
	}

	tdAsset struct {
		ID      tdAssetID
		Expired bool
		Parents []tdParentOf
		Owners  []tdOwns
	}

	tdAssetID struct {
		Type       string
		Identifier string
	}

	tdParentOf struct {
		Parent  tdAssetID
		Expired bool
	}

	tdOwns struct {
		Team    string
		Expired bool
	}
)

var (
	want = testdata{
		Teams: []tdTeam{
			{
				Identifier: "team0",
				Name:       "team0 name",
			},
			{
				Identifier: "team1",
				Name:       "team1 name",
			},
		},
		Assets: []tdAsset{
			{
				ID: tdAssetID{
					Type:       "Hostname",
					Identifier: "asset0.example.com",
				},
				Expired: false,
				Parents: []tdParentOf{
					{
						Parent: tdAssetID{
							Type:       "AWSAccount",
							Identifier: "aws0",
						},
						Expired: false,
					},
				},
				Owners: []tdOwns{
					{
						Team:    "team0",
						Expired: false,
					},
					{
						Team:    "team1",
						Expired: true,
					},
				},
			},
			{
				ID: tdAssetID{
					Type:       "Hostname",
					Identifier: "asset1.example.com",
				},
				Expired: false,
				Parents: []tdParentOf{
					{
						Parent: tdAssetID{
							Type:       "AWSAccount",
							Identifier: "aws0",
						},
						Expired: false,
					},
				},
				Owners: []tdOwns{
					{
						Team:    "team0",
						Expired: false,
					},
				},
			},
			{
				ID: tdAssetID{
					Type:       "Hostname",
					Identifier: "asset2.example.com",
				},
				Expired: false,
				Parents: []tdParentOf{
					{
						Parent: tdAssetID{
							Type:       "AWSAccount",
							Identifier: "aws0",
						},
						Expired: false,
					},
				},
				Owners: []tdOwns{
					{
						Team:    "team0",
						Expired: false,
					},
				},
			},
			{
				ID: tdAssetID{
					Type:       "Hostname",
					Identifier: "asset3.example.com",
				},
				Expired: false,
				Parents: []tdParentOf{
					{
						Parent: tdAssetID{
							Type:       "AWSAccount",
							Identifier: "aws1",
						},
						Expired: true,
					},
				},
				Owners: []tdOwns{
					{
						Team:    "team0",
						Expired: false,
					},
					{
						Team:    "team1",
						Expired: false,
					},
				},
			},
			{
				ID: tdAssetID{
					Type:       "Hostname",
					Identifier: "asset4.example.com",
				},
				Expired: true,
				Parents: []tdParentOf{
					{
						Parent: tdAssetID{
							Type:       "AWSAccount",
							Identifier: "aws2",
						},
						Expired: true,
					},
				},
				Owners: []tdOwns{
					{
						Team:    "team1",
						Expired: true,
					},
				},
			},
			{
				ID: tdAssetID{
					Type:       "AWSAccount",
					Identifier: "aws0",
				},
				Expired: false,
				Parents: nil,
				Owners: []tdOwns{
					{
						Team:    "team0",
						Expired: false,
					},
				},
			},
			{
				ID: tdAssetID{
					Type:       "AWSAccount",
					Identifier: "aws1",
				},
				Expired: true,
				Parents: nil,
				Owners: []tdOwns{
					{
						Team:    "team0",
						Expired: true,
					},
					{
						Team:    "team1",
						Expired: true,
					},
				},
			},
			{
				ID: tdAssetID{
					Type:       "AWSAccount",
					Identifier: "aws2",
				},
				Expired: false,
				Parents: nil,
				Owners: []tdOwns{
					{
						Team:    "team1",
						Expired: false,
					},
				},
			},
		},
	}

	diffOpts = []cmp.Option{
		cmpopts.SortSlices(func(a, b tdTeam) bool {
			return a.Identifier < b.Identifier
		}),
		cmpopts.SortSlices(func(a, b tdAsset) bool {
			idA := a.ID.Type + "/" + a.ID.Identifier
			idB := b.ID.Type + "/" + b.ID.Identifier
			return idA < idB
		}),
		cmpopts.SortSlices(func(a, b tdParentOf) bool {
			idA := a.Parent.Type + "/" + a.Parent.Identifier
			idB := b.Parent.Type + "/" + b.Parent.Identifier
			return idA < idB
		}),
		cmpopts.SortSlices(func(a, b tdOwns) bool {
			return a.Team < b.Team
		}),
	}
)

func TestMain(t *testing.T) {
	if err := setupKafka(); err != nil {
		t.Fatalf("error setting up kafka: %v", err)
	}

	if err := resetInventory(); err != nil {
		t.Fatalf("error resetting inventory: %v", err)
	}

	cfg := config{
		logLevel:                    "disabled",
		retryDuration:               0,
		kafkaBootstrapServers:       "127.0.0.1:9092",
		kafkaGroupID:                "cmd-graph-vulcan-assets-main-test",
		kafkaUsername:               "",
		kafkaPassword:               "",
		inventoryEndpoint:           "http://127.0.0.1:8000",
		inventoryInsecureSkipVerify: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		// Ignore this error. The last message of testdata has an
		// invalid payload to force run to return.
		t.Logf("error processing messages: %v", err)
	}

	icli, err := inventory.NewClient(cfg.inventoryEndpoint, cfg.inventoryInsecureSkipVerify)
	if err != nil {
		t.Fatalf("could not create inventory client: %v", err)
	}

	got, err := getTestResults(icli)
	if err != nil {
		t.Fatalf("error getting test results: %v", err)
	}

	if diff := cmp.Diff(want, got, diffOpts...); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%v", diff)
	}
}

func getTestResults(icli inventory.Client) (testdata, error) {
	var td testdata

	// Get teams.
	teams, err := icli.Teams("", inventory.Pagination{})
	if err != nil {
		return testdata{}, fmt.Errorf("could not get teams: %w", err)
	}

	for _, t := range teams {
		tdt := tdTeam{
			Identifier: t.Identifier,
			Name:       t.Name,
		}
		td.Teams = append(td.Teams, tdt)
	}

	// Get assets.
	assets, err := icli.Assets("", "", time.Time{}, inventory.Pagination{})
	if err != nil {
		return testdata{}, fmt.Errorf("could not get assets: %w", err)
	}

	for _, a := range assets {
		tda, err := getTestAsset(icli, assets, teams, a)
		if err != nil {
			return testdata{}, fmt.Errorf("could not get asset: %w", err)
		}
		td.Assets = append(td.Assets, tda)
	}

	return td, nil
}

func getTestAsset(icli inventory.Client, assets []inventory.AssetResp, teams []inventory.TeamResp, asset inventory.AssetResp) (tdAsset, error) {
	tda := tdAsset{
		ID: tdAssetID{
			Type:       asset.Type,
			Identifier: asset.Identifier,
		},
		Expired: !asset.Expiration.Equal(inventory.Unexpired),
	}

	// Get parents.
	parents, err := icli.Parents(asset.ID, inventory.Pagination{})
	if err != nil {
		return tdAsset{}, fmt.Errorf("could not get parents: %w", err)
	}

	for _, p := range parents {
		parent, err := findAsset(assets, p.ParentID)
		if err != nil {
			return tdAsset{}, fmt.Errorf("could not find parent: %w", err)
		}
		tdp := tdParentOf{
			Parent: tdAssetID{
				Type:       parent.Type,
				Identifier: parent.Identifier,
			},
			Expired: !p.Expiration.Equal(inventory.Unexpired),
		}
		tda.Parents = append(tda.Parents, tdp)
	}

	// Get owners.
	owners, err := icli.Owners(asset.ID, inventory.Pagination{})
	if err != nil {
		return tdAsset{}, fmt.Errorf("could not get owners: %w", err)
	}

	for _, o := range owners {
		owner, err := findTeam(teams, o.TeamID)
		if err != nil {
			return tdAsset{}, fmt.Errorf("could not find owner: %w", err)
		}
		tdo := tdOwns{
			Team:    owner.Identifier,
			Expired: o.EndTime != nil,
		}
		tda.Owners = append(tda.Owners, tdo)
	}

	return tda, nil
}

func findAsset(assets []inventory.AssetResp, id string) (inventory.AssetResp, error) {
	for _, a := range assets {
		if a.ID == id {
			return a, nil
		}
	}
	return inventory.AssetResp{}, errors.New("not found")
}

func findTeam(teams []inventory.TeamResp, id string) (inventory.TeamResp, error) {
	for _, t := range teams {
		if t.ID == id {
			return t, nil
		}
	}
	return inventory.TeamResp{}, errors.New("not found")
}
