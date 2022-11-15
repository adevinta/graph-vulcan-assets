// graph-vulcan-assets is a consumer of the Vulcan Asynchronous API that keeps
// in sync the Security Graph Asset Inventory.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/adevinta/graph-vulcan-assets/inventory"
	"github.com/adevinta/graph-vulcan-assets/log"
	"github.com/adevinta/graph-vulcan-assets/stream/kafka"
	"github.com/adevinta/graph-vulcan-assets/vulcan"
)

// TODO(rm): The current implementation requires a lot of requests against the
// Asset Inventory API every time an asset is updated.

const (
	defaultLogLevel      = "info"
	defaultRetryDuration = 5 * time.Second
	defaultKafkaGroupID  = "graph-vulcan-assets"
)

func main() {
	cfg, err := readConfig()
	if err != nil {
		log.Fatalf("graph-vulcan-assets: error reading config: %v", err)
	}

	if err := run(context.Background(), cfg); err != nil {
		log.Fatalf("graph-vulcan-assets: %v", err)
	}
}

// run is invoked by main and does the actual work.
func run(ctx context.Context, cfg config) error {
	if err := log.SetLevel(cfg.LogLevel); err != nil {
		return fmt.Errorf("error setting log level: %w", err)
	}

	kcfg := map[string]any{
		"bootstrap.servers": cfg.KafkaBootstrapServers,
		"group.id":          cfg.KafkaGroupID,
		"auto.offset.reset": "earliest",
	}

	if cfg.KafkaUsername != "" && cfg.KafkaPassword != "" {
		kcfg["security.protocol"] = "sasl_ssl"
		kcfg["sasl.mechanisms"] = "SCRAM-SHA-256"
		kcfg["sasl.username"] = cfg.KafkaUsername
		kcfg["sasl.password"] = cfg.KafkaPassword
	}

	proc, err := kafka.NewAloProcessor(kcfg)
	if err != nil {
		return fmt.Errorf("error creating kafka processor: %w", err)
	}
	defer proc.Close()

	vcli := vulcan.NewClient(proc)

	icli, err := inventory.NewClient(cfg.InventoryEndpoint, cfg.InventoryInsecureSkipVerify)
	if err != nil {
		return fmt.Errorf("error creating asset inventory client: %w", err)
	}

	for {
		log.Info.Println("graph-vulcan-assets: processing assets")

		select {
		case <-ctx.Done():
			log.Info.Println("graph-vulcan-assets: context is done")
			return nil
		default:
		}

		if err := vcli.ProcessAssets(ctx, assetHandler(icli, cfg)); err != nil {
			err = fmt.Errorf("error processing assets: %w", err)
			if cfg.RetryDuration == 0 {
				return err
			}
			log.Error.Printf("graph-vulcan-assets: %v", err)
		}

		log.Info.Printf("graph-vulcan-assets: retrying in %v", cfg.RetryDuration)
		time.Sleep(cfg.RetryDuration)
	}
}

// assetHandler processes asset events coming from a stream.
func assetHandler(icli inventory.Client, cfg config) vulcan.AssetHandler {
	return func(payload vulcan.AssetPayload, isNil bool) error {
		log.Debug.Printf("graph-vulcan-assets: payload=%#v isNil=%v", payload, isNil)

		if isNil {
			if err := expireAsset(icli, payload); err != nil {
				return fmt.Errorf("could not expire asset: %w", err)
			}
			return nil
		}

		if err := refreshAsset(icli, payload, cfg); err != nil {
			return fmt.Errorf("could not refresh asset: %w", err)
		}

		return nil
	}
}

// refreshAsset is called when an asset is created or updated. It takes care of
// refreshing its time attributes, as well as its parent-of and owns relations.
func refreshAsset(icli inventory.Client, payload vulcan.AssetPayload, cfg config) error {
	asset, err := upsertAsset(icli, payload)
	if err != nil {
		return fmt.Errorf("could not upsert asset: %w", err)
	}

	team, err := upsertTeam(icli, payload)
	if err != nil {
		return fmt.Errorf("could not upsert team: %w", err)
	}

	if err := setOwner(icli, asset, team); err != nil {
		return fmt.Errorf("could not set owner: %w", err)
	}

	for _, a := range payload.Annotations {
		if a.Key != cfg.AWSAccountAnnotationKey {
			continue
		}
		if err := setAWSAccount(icli, asset, a.Value); err != nil {
			return fmt.Errorf("could not set AWS account: %w", err)
		}
	}

	return nil
}

// upsertAsset creates an asset if it does not exist. If it exists, it updates
// its time attributes. It returns the created or updated asset.
func upsertAsset(icli inventory.Client, payload vulcan.AssetPayload) (inventory.AssetResp, error) {
	assets, err := icli.Assets(string(payload.AssetType), payload.Identifier, time.Time{}, inventory.Pagination{})
	if err != nil {
		return inventory.AssetResp{}, fmt.Errorf("could not get assets: %w", err)
	}

	switch len(assets) {
	case 1:
		asset, err := icli.UpdateAsset(assets[0].ID, string(payload.AssetType), payload.Identifier, time.Now(), inventory.Unexpired)
		if err != nil {
			return inventory.AssetResp{}, fmt.Errorf("could not update asset: %w", err)
		}
		return asset, nil
	case 0:
		asset, err := icli.CreateAsset(string(payload.AssetType), payload.Identifier, time.Now(), inventory.Unexpired)
		if err != nil {
			return inventory.AssetResp{}, fmt.Errorf("could not create asset: %w", err)
		}
		return asset, nil
	}

	return inventory.AssetResp{}, errors.New("duplicated asset")
}

// upsertTeam creates a team if it does not exist. If it exists, it updates its
// name. It returns the created or updated team.
func upsertTeam(icli inventory.Client, payload vulcan.AssetPayload) (inventory.TeamResp, error) {
	vteam := payload.Team

	teams, err := icli.Teams(vteam.ID, inventory.Pagination{})
	if err != nil {
		return inventory.TeamResp{}, fmt.Errorf("could not get teams: %w", err)
	}

	switch len(teams) {
	case 1:
		team, err := icli.UpdateTeam(teams[0].ID, vteam.ID, vteam.Name)
		if err != nil {
			return inventory.TeamResp{}, fmt.Errorf("could not update team: %w", err)
		}
		return team, nil
	case 0:
		team, err := icli.CreateTeam(vteam.ID, vteam.Name)
		if err != nil {
			return inventory.TeamResp{}, fmt.Errorf("could not create team: %w", err)
		}
		return team, nil
	default:
		return inventory.TeamResp{}, errors.New("duplicated team")
	}
}

// setOwner sets the owner of an assset. If the owns relation already exists,
// the original [inventory.OwnsResp.StartTime] is used.
func setOwner(icli inventory.Client, asset inventory.AssetResp, team inventory.TeamResp) error {
	owners, err := icli.Owners(asset.ID, inventory.Pagination{})
	if err != nil {
		return fmt.Errorf("could not get owners: %w", err)
	}

	startTime := time.Now()
	for _, o := range owners {
		if o.TeamID == team.ID {
			startTime = o.StartTime
			break
		}
	}

	if _, err := icli.UpsertOwner(asset.ID, team.ID, startTime, time.Time{}); err != nil {
		return fmt.Errorf("could not upsert owner: %w", err)
	}

	return nil
}

// setAWSAccount sets the parent AWS account of an assset. It takes care of
// normalizing the AWS account ID, so it always has the long format
// "arn:aws:iam::000000000000:root".
func setAWSAccount(icli inventory.Client, asset inventory.AssetResp, awsAccount string) error {
	normAWSAccount, err := normalizeAWSAccountID(awsAccount)
	if err != nil {
		return fmt.Errorf("could not normalize AWS account ID: %w", err)
	}

	payload := vulcan.AssetPayload{
		Identifier: normAWSAccount,
		AssetType:  vulcan.AssetType("AWSAccount"),
	}
	assetAWSAccount, err := upsertAsset(icli, payload)
	if err != nil {
		return fmt.Errorf("could not upsert AWS account: %w", err)
	}

	if _, err := icli.UpsertParent(asset.ID, assetAWSAccount.ID, time.Now(), inventory.Unexpired); err != nil {
		return fmt.Errorf("could not upsert parent: %w", err)
	}

	return nil
}

var (
	shortAWSAccountRe = regexp.MustCompile(`^[0-9]{12}$`)
	longAWSAccountRe  = regexp.MustCompile(`^arn:aws:iam::[0-9]{12}:root$`)
)

// normalizeAWSAccountID normalizes the provided AWS account ID. The returned
// ID will always follows the format "arn:aws:iam::000000000000:root".
func normalizeAWSAccountID(id string) (string, error) {
	if longAWSAccountRe.MatchString(id) {
		return id, nil
	}

	if shortAWSAccountRe.MatchString(id) {
		return fmt.Sprintf("arn:aws:iam::%v:root", id), nil
	}

	return "", fmt.Errorf("invalid AWS account id format: %v", id)
}

// expireAsset expires the provided asset, which means:
//
//   - The owns relation with the specific team is expired.
//   - If all the owns relations are expired, the asset is expired.
//   - If the asset is expired, all its parent-of relations are expired (both
//     ingoing and outgoing).
func expireAsset(icli inventory.Client, payload vulcan.AssetPayload) error {
	assets, err := icli.Assets(string(payload.AssetType), payload.Identifier, time.Time{}, inventory.Pagination{})
	if err != nil {
		return fmt.Errorf("could not get assets: %w", err)
	}

	if len(assets) == 0 {
		// The asset does not exist, so nothing needs to be done.
		return nil
	}
	if len(assets) > 1 {
		return errors.New("duplicated asset")
	}

	teams, err := icli.Teams(payload.Team.ID, inventory.Pagination{})
	if err != nil {
		return fmt.Errorf("could not get teams: %w", err)
	}

	if len(teams) == 0 {
		// The team does not exist, so nothing needs to be done.
		return nil
	}
	if len(teams) > 1 {
		return errors.New("duplicated team")
	}

	now := time.Now()

	// Check if there is any active owns relation end expire owner.
	owners, err := icli.Owners(assets[0].ID, inventory.Pagination{})
	if err != nil {
		return fmt.Errorf("error getting owners: %w", err)
	}

	var active bool
	for _, o := range owners {
		if o.TeamID != teams[0].ID {
			if o.EndTime == nil {
				active = true
			}
			continue
		}

		if _, err := icli.UpsertOwner(assets[0].ID, teams[0].ID, o.StartTime, now); err != nil {
			return fmt.Errorf("could not expire owner: %w", err)
		}
	}

	// If the asset is still owned by a team, we can return because it is
	// not expired.
	if active {
		return nil
	}

	// Expire asset.
	asset, err := icli.UpdateAsset(assets[0].ID, string(payload.AssetType), payload.Identifier, now, now)
	if err != nil {
		return fmt.Errorf("could not expire asset: %w", err)
	}

	// Expire parents.
	parents, err := icli.Parents(asset.ID, inventory.Pagination{})
	if err != nil {
		return fmt.Errorf("could not get parents: %w", err)
	}

	for _, p := range parents {
		if p.Expiration.Before(now) || p.Expiration.Equal(now) {
			continue
		}

		if _, err := icli.UpsertParent(p.ChildID, p.ParentID, now, now); err != nil {
			return fmt.Errorf("error expiring parent-of relations: %w", err)
		}
	}

	// Expire children.
	children, err := icli.Children(asset.ID, inventory.Pagination{})
	if err != nil {
		return fmt.Errorf("could not get children: %w", err)
	}

	for _, c := range children {
		if c.Expiration.Before(now) || c.Expiration.Equal(now) {
			continue
		}

		if _, err := icli.UpsertParent(c.ChildID, c.ParentID, now, now); err != nil {
			return fmt.Errorf("error expiring parent-of relations: %w", err)
		}
	}

	return nil
}

// config contains the configuration of the command.
type config struct {
	LogLevel                    string
	RetryDuration               time.Duration
	KafkaBootstrapServers       string
	KafkaGroupID                string
	KafkaUsername               string
	KafkaPassword               string
	AWSAccountAnnotationKey     string
	InventoryEndpoint           string
	InventoryInsecureSkipVerify bool
}

// readConfig reads the configuration from the environment.
func readConfig() (config, error) {
	// Required config.
	kafkaBootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if kafkaBootstrapServers == "" {
		return config{}, errors.New("missing kafka bootstrap servers")
	}

	inventoryEndpoint := os.Getenv("INVENTORY_ENDPOINT")
	if inventoryEndpoint == "" {
		return config{}, errors.New("missing asset inventory endpoint")
	}

	awsAccountAnnotationKey := os.Getenv("AWS_ACCOUNT_ANNOTATION_KEY")
	if awsAccountAnnotationKey == "" {
		return config{}, errors.New("missing AWS account annotation key")
	}

	// Optional config.
	logLevel := defaultLogLevel
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		logLevel = level
	}

	retryDuration := defaultRetryDuration
	if rd := os.Getenv("RETRY_DURATION"); rd != "" {
		var err error

		retryDuration, err = time.ParseDuration(rd)
		if err != nil {
			return config{}, fmt.Errorf("invalid retry duration: %w", err)
		}
	}

	kafkaGroupID := defaultKafkaGroupID
	if id := os.Getenv("KAFKA_GROUP_ID"); id != "" {
		kafkaGroupID = id
	}

	kafkaUsername := os.Getenv("KAFKA_USERNAME")
	kafkaPassword := os.Getenv("KAFKA_PASSWORD")

	inventoryInsecureSkipVerify := os.Getenv("INVENTORY_INSECURE_SKIP_VERIFY") == "1"

	cfg := config{
		LogLevel:                    logLevel,
		RetryDuration:               retryDuration,
		KafkaBootstrapServers:       kafkaBootstrapServers,
		KafkaGroupID:                kafkaGroupID,
		KafkaUsername:               kafkaUsername,
		KafkaPassword:               kafkaPassword,
		AWSAccountAnnotationKey:     awsAccountAnnotationKey,
		InventoryEndpoint:           inventoryEndpoint,
		InventoryInsecureSkipVerify: inventoryInsecureSkipVerify,
	}

	return cfg, nil
}
