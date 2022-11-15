package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

	// endMessageKey is the key of the last message of testdata. It
	// contains an invalid payload to force run to return.
	endMessageKey = "ENDTESTDATA"
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
		return fmt.Errorf("error creating producer: %w", err)
	}
	defer prod.Close()

	admin, err := kafka.NewAdminClientFromProducer(prod)
	if err != nil {
		return fmt.Errorf("error creating admin client: %w", err)
	}

	adminOpts := []kafka.DeleteTopicsAdminOption{
		kafka.SetAdminRequestTimeout(timeout),
		kafka.SetAdminOperationTimeout(timeout),
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if _, err := admin.DeleteTopics(ctx, []string{vulcan.AssetsEntityName}, adminOpts...); err != nil {
		return fmt.Errorf("error deleting topic: %w", err)
	}

	msgs := streamtest.MustParse(messagesFile)
	for _, msg := range msgs {
		if err := produceMessage(prod, vulcan.AssetsEntityName, msg); err != nil {
			return fmt.Errorf("error producing message: %w", err)
		}
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
		return fmt.Errorf("failed to produce message: %w", err)
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
		return fmt.Errorf("could not connect to gremlin-server: %w", err)
	}
	defer conn.Close()

	g := gremlingo.Traversal_().WithRemote(conn)

	<-g.V().Not(gremlingo.T__.HasLabel("Universe")).Drop().Iterate()

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
			{
				Identifier: "team2",
				Name:       "team2 name",
			},
			{
				Identifier: "team3",
				Name:       "team3 name",
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
							Identifier: "arn:aws:iam::000000000000:root",
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
							Identifier: "arn:aws:iam::000000000000:root",
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
							Identifier: "arn:aws:iam::000000000000:root",
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
							Identifier: "arn:aws:iam::111111111111:root",
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
							Identifier: "arn:aws:iam::222222222222:root",
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
					Identifier: "arn:aws:iam::000000000000:root",
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
					Identifier: "arn:aws:iam::111111111111:root",
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
					Identifier: "arn:aws:iam::222222222222:root",
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
			{
				ID: tdAssetID{
					Type:       "Hostname",
					Identifier: "asset5.example.com",
				},
				Expired: false,
				Parents: nil,
				Owners: []tdOwns{
					{
						Team:    "team2",
						Expired: false,
					},
				},
			},
			{
				ID: tdAssetID{
					Type:       "Hostname",
					Identifier: "asset6.example.com",
				},
				Expired: false,
				Parents: nil,
				Owners: []tdOwns{
					{
						Team:    "team3",
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

func TestRun(t *testing.T) {
	if err := setupKafka(); err != nil {
		t.Fatalf("error setting up kafka: %v", err)
	}

	if err := resetInventory(); err != nil {
		t.Fatalf("error resetting inventory: %v", err)
	}

	cfg := config{
		LogLevel:                    "disabled",
		RetryDuration:               0,
		KafkaBootstrapServers:       "127.0.0.1:9092",
		KafkaGroupID:                "cmd-graph-vulcan-assets-main-test",
		KafkaUsername:               "",
		KafkaPassword:               "",
		AWSAccountAnnotationKey:     "discovery/aws/account",
		InventoryEndpoint:           "http://127.0.0.1:8000",
		InventoryInsecureSkipVerify: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		if !strings.Contains(err.Error(), endMessageKey) {
			t.Fatalf("error processing messages: %v", err)
		}
	}

	icli, err := inventory.NewClient(cfg.InventoryEndpoint, cfg.InventoryInsecureSkipVerify)
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

func TestReadConfig(t *testing.T) {
	tests := []struct {
		name       string
		env        map[string]string
		wantConfig config
		wantNilErr bool
	}{
		{
			name: "set required config",
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS":    "127.0.0.1:9092",
				"INVENTORY_ENDPOINT":         "http://127.0.0.1:8000",
				"AWS_ACCOUNT_ANNOTATION_KEY": "discovery/aws/account",
			},
			wantConfig: config{
				LogLevel:                    defaultLogLevel,
				RetryDuration:               defaultRetryDuration,
				KafkaBootstrapServers:       "127.0.0.1:9092",
				KafkaGroupID:                defaultKafkaGroupID,
				KafkaUsername:               "",
				KafkaPassword:               "",
				AWSAccountAnnotationKey:     "discovery/aws/account",
				InventoryEndpoint:           "http://127.0.0.1:8000",
				InventoryInsecureSkipVerify: false,
			},
			wantNilErr: true,
		},
		{
			name: "set optional config",
			env: map[string]string{
				"LOG_LEVEL":                      "debug",
				"RETRY_DURATION":                 "30s",
				"KAFKA_BOOTSTRAP_SERVERS":        "127.0.0.1:9092",
				"KAFKA_GROUP_ID":                 "group-id",
				"KAFKA_USERNAME":                 "username",
				"KAFKA_PASSWORD":                 "password",
				"AWS_ACCOUNT_ANNOTATION_KEY":     "discovery/aws/account",
				"INVENTORY_ENDPOINT":             "http://127.0.0.1:8000",
				"INVENTORY_INSECURE_SKIP_VERIFY": "1",
			},
			wantConfig: config{
				LogLevel:                    "debug",
				RetryDuration:               30 * time.Second,
				KafkaBootstrapServers:       "127.0.0.1:9092",
				KafkaGroupID:                "group-id",
				KafkaUsername:               "username",
				KafkaPassword:               "password",
				AWSAccountAnnotationKey:     "discovery/aws/account",
				InventoryEndpoint:           "http://127.0.0.1:8000",
				InventoryInsecureSkipVerify: true,
			},
			wantNilErr: true,
		},
		{
			name: "missing KAFKA_BOOTSTRAP_SERVERS",
			env: map[string]string{
				"INVENTORY_ENDPOINT":         "http://127.0.0.1:8000",
				"AWS_ACCOUNT_ANNOTATION_KEY": "discovery/aws/account",
			},
			wantConfig: config{},
			wantNilErr: false,
		},
		{
			name: "missing INVENTORY_ENDPOINT",
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS":    "127.0.0.1:9092",
				"AWS_ACCOUNT_ANNOTATION_KEY": "discovery/aws/account",
			},
			wantConfig: config{},
			wantNilErr: false,
		},
		{
			name: "missing AWS_ACCOUNT_ANNOTATION_KEY",
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:9092",
				"INVENTORY_ENDPOINT":      "http://127.0.0.1:8000",
			},
			wantConfig: config{},
			wantNilErr: false,
		},
		{
			name: "invalid RETRY_DURATION",
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS":    "127.0.0.1:9092",
				"INVENTORY_ENDPOINT":         "http://127.0.0.1:8000",
				"AWS_ACCOUNT_ANNOTATION_KEY": "discovery/aws/account",
				"RETRY_DURATION":             "30x",
			},
			wantConfig: config{},
			wantNilErr: false,
		},
		{
			name: "zero RETRY_DURATION",
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS":    "127.0.0.1:9092",
				"INVENTORY_ENDPOINT":         "http://127.0.0.1:8000",
				"AWS_ACCOUNT_ANNOTATION_KEY": "discovery/aws/account",
				"RETRY_DURATION":             "0",
			},
			wantConfig: config{
				LogLevel:                    defaultLogLevel,
				RetryDuration:               0,
				KafkaBootstrapServers:       "127.0.0.1:9092",
				KafkaGroupID:                defaultKafkaGroupID,
				KafkaUsername:               "",
				KafkaPassword:               "",
				AWSAccountAnnotationKey:     "discovery/aws/account",
				InventoryEndpoint:           "http://127.0.0.1:8000",
				InventoryInsecureSkipVerify: false,
			},
			wantNilErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			config, err := readConfig()
			if (err == nil) != tt.wantNilErr {
				t.Errorf("unexpected error: wantNilErr=%v, got=%v", tt.wantNilErr, err)
			}

			if diff := cmp.Diff(tt.wantConfig, config); diff != "" {
				t.Errorf("messages mismatch (-want +got):\n%v", diff)
			}
		})
	}
}

func TestNormalizeAWSAccountID(t *testing.T) {
	tests := []struct {
		name       string
		id         string
		wantID     string
		wantNilErr bool
	}{
		{
			name:       "long ID",
			id:         "arn:aws:iam::123456789012:root",
			wantID:     "arn:aws:iam::123456789012:root",
			wantNilErr: true,
		},
		{
			name:       "short ID",
			id:         "123456789012",
			wantID:     "arn:aws:iam::123456789012:root",
			wantNilErr: true,
		},
		{
			name:       "invalid long ID",
			id:         "arn:aws:iam::12345abc9012:root",
			wantID:     "",
			wantNilErr: false,
		},
		{
			name:       "invalid short ID",
			id:         "12345abc9012",
			wantID:     "",
			wantNilErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, err := normalizeAWSAccountID(tt.id)

			if (err == nil) != tt.wantNilErr {
				t.Errorf("unexpected error: wantNilErr=%v, got=%v", tt.wantNilErr, err)
			}

			if gotID != tt.wantID {
				t.Errorf("unexpected ID: want=%v, got=%v", tt.wantID, gotID)
			}
		})
	}
}
