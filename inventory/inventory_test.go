package inventory

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const (
	inventoryEndpoint = "http://127.0.0.1:8000"
	gremlinEndpoint   = "ws://127.0.0.1:8182/gremlin"
)

func resetGraph() error {
	conn, err := gremlingo.NewDriverRemoteConnection(gremlinEndpoint, func(settings *gremlingo.DriverRemoteConnectionSettings) {
		settings.LogVerbosity = gremlingo.Off
	})
	if err != nil {
		return fmt.Errorf("could not connect to gremlin-server: %w", err)
	}
	defer conn.Close()

	g := gremlingo.Traversal_().WithRemote(conn)

	g.V().Not(gremlingo.T__.HasLabel("Universe")).Drop().Next()

	return nil
}

var (
	teamsTestdata = []TeamReq{
		{
			Identifier: "Identifier0",
			Name:       "Name0",
		},
		{
			Identifier: "Identifier1",
			Name:       "Name1",
		},
		{
			Identifier: "Identifier2",
			Name:       "Name2",
		},
	}

	teamsWant = []TeamResp{
		{
			ID:         "ignored",
			Identifier: "Identifier0",
			Name:       "Name0",
		},
		{
			ID:         "ignored",
			Identifier: "Identifier1",
			Name:       "Name1",
		},
		{
			ID:         "ignored",
			Identifier: "Identifier2",
			Name:       "Name2",
		},
	}

	teamsDiffOpts = []cmp.Option{
		cmpopts.IgnoreFields(TeamResp{}, "ID"),
		cmpopts.SortSlices(func(a, b TeamResp) bool {
			return a.Identifier < b.Identifier
		}),
	}
)

func TestClientTeamsGetCreate(t *testing.T) {
	tests := []struct {
		name       string
		testdata   []TeamReq
		identifier string
		want       []TeamResp
	}{
		{
			name:       "get and create",
			testdata:   teamsTestdata,
			identifier: "",
			want:       teamsWant,
		},
		{
			name:       "filter",
			testdata:   teamsTestdata,
			identifier: "Identifier1",
			want:       teamsWant[1:2],
		},
		{
			name:       "partial identifier",
			testdata:   teamsTestdata,
			identifier: "Identifier",
			want:       []TeamResp{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := resetGraph(); err != nil {
				t.Fatalf("error setting up graph: %v", err)
			}

			cli, err := NewClient(inventoryEndpoint, true)
			if err != nil {
				t.Fatalf("error creating client: %v", err)
			}

			for _, td := range tt.testdata {
				if _, err := cli.CreateTeam(td.Identifier, td.Name); err != nil {
					t.Fatalf("error creating team: %v", err)
				}
			}

			teams, err := cli.Teams(tt.identifier, Pagination{})
			if err != nil {
				t.Fatalf("error getting teams: %v", err)
			}

			if diff := cmp.Diff(tt.want, teams, teamsDiffOpts...); diff != "" {
				t.Errorf("teams mismatch (-want +got):\n%v", diff)
			}
		})
	}
}

func TestClientTeamsPagination(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	for _, td := range teamsTestdata {
		if _, err := cli.CreateTeam(td.Identifier, td.Name); err != nil {
			t.Fatalf("error creating team: %v", err)
		}
	}

	var got []TeamResp
	for i := 0; i < len(teamsTestdata); i++ {
		teams, err := cli.Teams("", Pagination{Size: 1, Page: i})
		if err != nil {
			t.Fatalf("error getting teams: %v", err)
		}
		got = append(got, teams...)
	}

	if diff := cmp.Diff(teamsWant, got, teamsDiffOpts...); diff != "" {
		t.Errorf("teams mismatch (-want +got):\n%v", diff)
	}
}

func TestClientTeamsUpdate(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	team, err := cli.CreateTeam(
		"Identifier",
		"Name",
	)
	if err != nil {
		t.Fatalf("error creating team: %v", err)
	}

	_, err = cli.UpdateTeam(
		team.ID,
		team.Identifier,
		"NewName",
	)
	if err != nil {
		t.Fatalf("error updating team: %v", err)
	}

	want := []TeamResp{
		{
			Identifier: team.Identifier,
			Name:       "NewName",
		},
	}

	got, err := cli.Teams("", Pagination{})
	if err != nil {
		t.Fatalf("error getting teams: %v", err)
	}

	if diff := cmp.Diff(want, got, teamsDiffOpts...); diff != "" {
		t.Errorf("teams mismatch (-want +got):\n%v", diff)
	}
}

var (
	assetsTestdata = []AssetReq{
		{
			Type:       "Type0",
			Identifier: "Identifier0",
			Timestamp:  strtime("2020-01-01T12:00:00Z"),
			Expiration: *strtime("2020-02-01T12:00:00Z"),
		},
		{
			Type:       "Type1",
			Identifier: "Identifier1",
			Timestamp:  strtime("2021-01-01T12:00:00Z"),
			Expiration: *strtime("2021-02-01T12:00:00Z"),
		},
		{
			Type:       "Type2",
			Identifier: "Identifier2",
			Timestamp:  strtime("2022-01-01T12:00:00Z"),
			Expiration: *strtime("2022-02-01T12:00:00Z"),
		},
	}

	assetsWant = []AssetResp{
		{
			ID:         "ignored",
			Type:       "Type0",
			Identifier: "Identifier0",
			FirstSeen:  *strtime("2020-01-01T12:00:00Z"),
			LastSeen:   *strtime("2020-01-01T12:00:00Z"),
			Expiration: *strtime("2020-02-01T12:00:00Z"),
		},
		{
			ID:         "ignored",
			Type:       "Type1",
			Identifier: "Identifier1",
			FirstSeen:  *strtime("2021-01-01T12:00:00Z"),
			LastSeen:   *strtime("2021-01-01T12:00:00Z"),
			Expiration: *strtime("2021-02-01T12:00:00Z"),
		},
		{
			ID:         "ignored",
			Type:       "Type2",
			Identifier: "Identifier2",
			FirstSeen:  *strtime("2022-01-01T12:00:00Z"),
			LastSeen:   *strtime("2022-01-01T12:00:00Z"),
			Expiration: *strtime("2022-02-01T12:00:00Z"),
		},
	}

	assetsDiffOpts = []cmp.Option{
		cmpopts.IgnoreFields(AssetResp{}, "ID"),
		cmpopts.SortSlices(func(a, b AssetResp) bool {
			idA := a.Type + "/" + a.Identifier
			idB := b.Type + "/" + b.Identifier
			return idA < idB
		}),
	}
)

func TestClientAssetsGetCreate(t *testing.T) {
	tests := []struct {
		name       string
		testdata   []AssetReq
		typ        string
		identifier string
		validAt    time.Time
		want       []AssetResp
	}{
		{
			name:       "get and create",
			testdata:   assetsTestdata,
			typ:        "",
			identifier: "",
			validAt:    time.Time{},
			want:       assetsWant,
		},
		{
			name:       "filter",
			testdata:   assetsTestdata,
			typ:        "Type1",
			identifier: "Identifier1",
			validAt:    time.Time{},
			want:       assetsWant[1:2],
		},
		{
			name:       "valid at",
			testdata:   assetsTestdata,
			typ:        "",
			identifier: "",
			validAt:    *strtime("2022-01-15T12:00:00Z"),
			want:       assetsWant[2:3],
		},
		{
			name:       "partial type",
			testdata:   assetsTestdata,
			typ:        "Type",
			identifier: "Identifier1",
			validAt:    time.Time{},
			want:       []AssetResp{},
		},
		{
			name:       "partial identifier",
			testdata:   assetsTestdata,
			typ:        "Type1",
			identifier: "Identifier",
			validAt:    time.Time{},
			want:       []AssetResp{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := resetGraph(); err != nil {
				t.Fatalf("error setting up graph: %v", err)
			}

			cli, err := NewClient(inventoryEndpoint, true)
			if err != nil {
				t.Fatalf("error creating client: %v", err)
			}

			for _, td := range tt.testdata {
				if _, err := cli.CreateAsset(td.Type, td.Identifier, *td.Timestamp, td.Expiration); err != nil {
					t.Fatalf("error creating asset: %v", err)
				}
			}

			assets, err := cli.Assets(tt.typ, tt.identifier, tt.validAt, Pagination{})
			if err != nil {
				t.Fatalf("error getting assets: %v", err)
			}

			if diff := cmp.Diff(tt.want, assets, assetsDiffOpts...); diff != "" {
				t.Errorf("assets mismatch (-want +got):\n%v", diff)
			}
		})
	}
}

func TestClientAssetsPagination(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	for _, td := range assetsTestdata {
		if _, err := cli.CreateAsset(td.Type, td.Identifier, *td.Timestamp, td.Expiration); err != nil {
			t.Fatalf("error creating asset: %v", err)
		}
	}

	var got []AssetResp
	for i := 0; i < len(assetsTestdata); i++ {
		assets, err := cli.Assets("", "", time.Time{}, Pagination{Size: 1, Page: i})
		if err != nil {
			t.Fatalf("error getting assets: %v", err)
		}
		got = append(got, assets...)
	}

	if diff := cmp.Diff(assetsWant, got, assetsDiffOpts...); diff != "" {
		t.Errorf("assets mismatch (-want +got):\n%v", diff)
	}
}

func TestClientAssetsUpdate(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	asset, err := cli.CreateAsset(
		"Type",
		"Identifier",
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating asset: %v", err)
	}

	_, err = cli.UpdateAsset(
		asset.ID,
		asset.Type,
		asset.Identifier,
		*strtime("2025-01-01T12:00:00Z"),
		*strtime("2025-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error updating asset: %v", err)
	}

	want := []AssetResp{
		{
			Type:       asset.Type,
			Identifier: asset.Identifier,
			FirstSeen:  asset.FirstSeen,
			LastSeen:   *strtime("2025-01-01T12:00:00Z"),
			Expiration: *strtime("2025-02-01T12:00:00Z"),
		},
	}

	got, err := cli.Assets("", "", time.Time{}, Pagination{})
	if err != nil {
		t.Fatalf("error getting assets: %v", err)
	}

	if diff := cmp.Diff(want, got, assetsDiffOpts...); diff != "" {
		t.Errorf("assets mismatch (-want +got):\n%v", diff)
	}
}

var (
	parentsTestdata = []ParentOfReq{
		{
			Timestamp:  strtime("2020-01-01T12:00:00Z"),
			Expiration: *strtime("2020-02-01T12:00:00Z"),
		},
		{
			Timestamp:  strtime("2021-01-01T12:00:00Z"),
			Expiration: *strtime("2021-02-01T12:00:00Z"),
		},
		{
			Timestamp:  strtime("2022-01-01T12:00:00Z"),
			Expiration: *strtime("2022-02-01T12:00:00Z"),
		},
	}

	parentsWant = []ParentOfResp{
		{
			ID:         "ignored",
			ChildID:    "ignored",
			ParentID:   "ignored",
			FirstSeen:  *strtime("2020-01-01T12:00:00Z"),
			LastSeen:   *strtime("2020-01-01T12:00:00Z"),
			Expiration: *strtime("2020-02-01T12:00:00Z"),
		},
		{
			ID:         "ignored",
			ChildID:    "ignored",
			ParentID:   "ignored",
			FirstSeen:  *strtime("2021-01-01T12:00:00Z"),
			LastSeen:   *strtime("2021-01-01T12:00:00Z"),
			Expiration: *strtime("2021-02-01T12:00:00Z"),
		},
		{
			ID:         "ignored",
			ChildID:    "ignored",
			ParentID:   "ignored",
			FirstSeen:  *strtime("2022-01-01T12:00:00Z"),
			LastSeen:   *strtime("2022-01-01T12:00:00Z"),
			Expiration: *strtime("2022-02-01T12:00:00Z"),
		},
	}

	parentsDiffOpts = []cmp.Option{
		cmpopts.IgnoreFields(ParentOfResp{}, "ID", "ChildID", "ParentID"),
		cmpopts.SortSlices(func(a, b ParentOfResp) bool {
			return a.FirstSeen.Before(b.FirstSeen)
		}),
	}
)

func TestClientParentsGetCreate(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	child, err := cli.CreateAsset(
		"Type",
		"Identifier",
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating child asset: %v", err)
	}

	for i, td := range parentsTestdata {
		typ := "Type" + strconv.Itoa(i)
		identifier := "Identifier" + strconv.Itoa(i)
		parent, err := cli.CreateAsset(
			typ,
			identifier,
			*strtime("2022-01-01T12:00:00Z"),
			*strtime("2022-02-01T12:00:00Z"),
		)
		if err != nil {
			t.Fatalf("error creating parent asset: %v", err)
		}

		if _, err := cli.UpsertParent(child.ID, parent.ID, *td.Timestamp, td.Expiration); err != nil {
			t.Fatalf("error creating parent: %v", err)
		}
	}

	got, err := cli.Parents(child.ID, Pagination{})
	if err != nil {
		t.Fatalf("error getting parents: %v", err)
	}

	if diff := cmp.Diff(parentsWant, got, parentsDiffOpts...); diff != "" {
		t.Errorf("parents mismatch (-want +got):\n%v", diff)
	}
}

func TestClientParentsPagination(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	child, err := cli.CreateAsset(
		"Type",
		"Identifier",
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating child asset: %v", err)
	}

	for i, td := range parentsTestdata {
		typ := "Type" + strconv.Itoa(i)
		identifier := "Identifier" + strconv.Itoa(i)
		parent, err := cli.CreateAsset(
			typ,
			identifier,
			*strtime("2022-01-01T12:00:00Z"),
			*strtime("2022-02-01T12:00:00Z"),
		)
		if err != nil {
			t.Fatalf("error creating parent asset: %v", err)
		}

		if _, err := cli.UpsertParent(child.ID, parent.ID, *td.Timestamp, td.Expiration); err != nil {
			t.Fatalf("error creating parent: %v", err)
		}
	}

	var got []ParentOfResp
	for i := 0; i < len(parentsTestdata); i++ {
		parents, err := cli.Parents(child.ID, Pagination{Size: 1, Page: i})
		if err != nil {
			t.Fatalf("error getting parents: %v", err)
		}
		got = append(got, parents...)
	}

	if diff := cmp.Diff(parentsWant, got, parentsDiffOpts...); diff != "" {
		t.Errorf("parents mismatch (-want +got):\n%v", diff)
	}
}

func TestClientParentsUpdate(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	child, err := cli.CreateAsset(
		"TypeChild",
		"IdentifierChild",
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating child asset: %v", err)
	}

	parent, err := cli.CreateAsset(
		"TypeParent",
		"IdentifierParent",
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating parent asset: %v", err)
	}

	_, err = cli.UpsertParent(
		child.ID,
		parent.ID,
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating parent: %v", err)
	}

	_, err = cli.UpsertParent(
		child.ID,
		parent.ID,
		*strtime("2025-01-01T12:00:00Z"),
		*strtime("2025-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error updating parent: %v", err)
	}

	want := []ParentOfResp{
		{
			ID:         "ignored",
			ChildID:    "ignored",
			ParentID:   "ignored",
			FirstSeen:  *strtime("2022-01-01T12:00:00Z"),
			LastSeen:   *strtime("2025-01-01T12:00:00Z"),
			Expiration: *strtime("2025-02-01T12:00:00Z"),
		},
	}

	got, err := cli.Parents(child.ID, Pagination{})
	if err != nil {
		t.Fatalf("error getting parents: %v", err)
	}

	if diff := cmp.Diff(want, got, parentsDiffOpts...); diff != "" {
		t.Errorf("parents mismatch (-want +got):\n%v", diff)
	}
}

func TestClientChildren(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	parent, err := cli.CreateAsset(
		"Type",
		"Identifier",
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating parent asset: %v", err)
	}

	for i, td := range parentsTestdata {
		typ := "Type" + strconv.Itoa(i)
		identifier := "Identifier" + strconv.Itoa(i)
		child, err := cli.CreateAsset(
			typ,
			identifier,
			*strtime("2022-01-01T12:00:00Z"),
			*strtime("2022-02-01T12:00:00Z"),
		)
		if err != nil {
			t.Fatalf("error creating child asset: %v", err)
		}

		if _, err := cli.UpsertParent(child.ID, parent.ID, *td.Timestamp, td.Expiration); err != nil {
			t.Fatalf("error creating parent: %v", err)
		}
	}

	got, err := cli.Children(parent.ID, Pagination{})
	if err != nil {
		t.Fatalf("error getting parents: %v", err)
	}

	if diff := cmp.Diff(parentsWant, got, parentsDiffOpts...); diff != "" {
		t.Errorf("parents mismatch (-want +got):\n%v", diff)
	}
}

func TestClientChildrenPagination(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	parent, err := cli.CreateAsset(
		"Type",
		"Identifier",
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating parent asset: %v", err)
	}

	for i, td := range parentsTestdata {
		typ := "Type" + strconv.Itoa(i)
		identifier := "Identifier" + strconv.Itoa(i)
		child, err := cli.CreateAsset(
			typ,
			identifier,
			*strtime("2022-01-01T12:00:00Z"),
			*strtime("2022-02-01T12:00:00Z"),
		)
		if err != nil {
			t.Fatalf("error creating child asset: %v", err)
		}

		if _, err := cli.UpsertParent(child.ID, parent.ID, *td.Timestamp, td.Expiration); err != nil {
			t.Fatalf("error creating parent: %v", err)
		}
	}

	var got []ParentOfResp
	for i := 0; i < len(parentsTestdata); i++ {
		children, err := cli.Children(parent.ID, Pagination{Size: 1, Page: i})
		if err != nil {
			t.Fatalf("error getting children: %v", err)
		}
		got = append(got, children...)
	}

	if diff := cmp.Diff(parentsWant, got, parentsDiffOpts...); diff != "" {
		t.Errorf("parents mismatch (-want +got):\n%v", diff)
	}
}

var (
	ownersTestdata = []OwnsReq{
		{
			StartTime: *strtime("2020-01-01T12:00:00Z"),
			EndTime:   strtime("2020-02-01T12:00:00Z"),
		},
		{
			StartTime: *strtime("2021-01-01T12:00:00Z"),
			EndTime:   strtime("2021-02-01T12:00:00Z"),
		},
		{
			StartTime: *strtime("2022-01-01T12:00:00Z"),
			EndTime:   strtime("2022-02-01T12:00:00Z"),
		},
	}

	ownersWant = []OwnsResp{
		{
			ID:        "ignored",
			AssetID:   "ignored",
			TeamID:    "ignored",
			StartTime: *strtime("2020-01-01T12:00:00Z"),
			EndTime:   strtime("2020-02-01T12:00:00Z"),
		},
		{
			ID:        "ignored",
			AssetID:   "ignored",
			TeamID:    "ignored",
			StartTime: *strtime("2021-01-01T12:00:00Z"),
			EndTime:   strtime("2021-02-01T12:00:00Z"),
		},
		{
			ID:        "ignored",
			AssetID:   "ignored",
			TeamID:    "ignored",
			StartTime: *strtime("2022-01-01T12:00:00Z"),
			EndTime:   strtime("2022-02-01T12:00:00Z"),
		},
	}

	ownersDiffOpts = []cmp.Option{
		cmpopts.IgnoreFields(OwnsResp{}, "ID", "AssetID", "TeamID"),
		cmpopts.SortSlices(func(a, b OwnsResp) bool {
			return a.StartTime.Before(b.StartTime)
		}),
	}
)

func TestClientOwnersGetCreate(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	asset, err := cli.CreateAsset(
		"Type",
		"Identifier",
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating asset: %v", err)
	}

	for i, td := range ownersTestdata {
		identifier := "Identifier" + strconv.Itoa(i)
		name := "Name" + strconv.Itoa(i)
		team, err := cli.CreateTeam(identifier, name)
		if err != nil {
			t.Fatalf("error creating team: %v", err)
		}

		if _, err := cli.UpsertOwner(asset.ID, team.ID, td.StartTime, *td.EndTime); err != nil {
			t.Fatalf("error creating owner: %v", err)
		}
	}

	got, err := cli.Owners(asset.ID, Pagination{})
	if err != nil {
		t.Fatalf("error getting owners: %v", err)
	}

	if diff := cmp.Diff(ownersWant, got, ownersDiffOpts...); diff != "" {
		t.Errorf("owners mismatch (-want +got):\n%v", diff)
	}
}

func TestClientOwnersPagination(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	asset, err := cli.CreateAsset(
		"Type",
		"Identifier",
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating asset: %v", err)
	}

	for i, td := range ownersTestdata {
		identifier := "Identifier" + strconv.Itoa(i)
		name := "Name" + strconv.Itoa(i)
		team, err := cli.CreateTeam(identifier, name)
		if err != nil {
			t.Fatalf("error creating team: %v", err)
		}

		if _, err := cli.UpsertOwner(asset.ID, team.ID, td.StartTime, *td.EndTime); err != nil {
			t.Fatalf("error creating owner: %v", err)
		}
	}

	var got []OwnsResp
	for i := 0; i < len(ownersTestdata); i++ {
		owners, err := cli.Owners(asset.ID, Pagination{Size: 1, Page: i})
		if err != nil {
			t.Fatalf("error getting owners: %v", err)
		}
		got = append(got, owners...)
	}

	if diff := cmp.Diff(ownersWant, got, ownersDiffOpts...); diff != "" {
		t.Errorf("owners mismatch (-want +got):\n%v", diff)
	}
}

func TestClientOwnersUpdate(t *testing.T) {
	if err := resetGraph(); err != nil {
		t.Fatalf("error setting up graph: %v", err)
	}

	cli, err := NewClient(inventoryEndpoint, true)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	asset, err := cli.CreateAsset(
		"Type",
		"Identifier",
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating asset: %v", err)
	}

	team, err := cli.CreateTeam("Identifier", "Name")
	if err != nil {
		t.Fatalf("error creating team: %v", err)
	}

	_, err = cli.UpsertOwner(
		asset.ID,
		team.ID,
		*strtime("2022-01-01T12:00:00Z"),
		*strtime("2022-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error creating owner: %v", err)
	}

	_, err = cli.UpsertOwner(
		asset.ID,
		team.ID,
		*strtime("2025-01-01T12:00:00Z"),
		*strtime("2025-02-01T12:00:00Z"),
	)
	if err != nil {
		t.Fatalf("error updating owner: %v", err)
	}

	want := []OwnsResp{
		{
			ID:        "ignored",
			AssetID:   "ignored",
			TeamID:    "ignored",
			StartTime: *strtime("2025-01-01T12:00:00Z"),
			EndTime:   strtime("2025-02-01T12:00:00Z"),
		},
	}

	got, err := cli.Owners(asset.ID, Pagination{})
	if err != nil {
		t.Fatalf("error getting owners: %v", err)
	}

	if diff := cmp.Diff(want, got, ownersDiffOpts...); diff != "" {
		t.Errorf("owners mismatch (-want +got):\n%v", diff)
	}
}
