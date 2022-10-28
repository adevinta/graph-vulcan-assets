package vulcan

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/adevinta/graph-vulcan-assets/stream/streamtest"
)

type asset struct {
	Id      string
	Payload AssetPayload
}

// testdataValidAssets must be in sync with testdata/valid_assets.dat.
var testdataValidAssets = []asset{
	{
		Id: "9a1a0332-88b6-4edc-aa37-50adc1ad96da/f110cf6f-803d-442c-9b42-f6d8cd962bf2",
		Payload: AssetPayload{
			Id: "f110cf6f-803d-442c-9b42-f6d8cd962bf2",
			Team: &Team{
				Id:          "9a1a0332-88b6-4edc-aa37-50adc1ad96da",
				Name:        "Team name 0",
				Description: "Team description 0",
				Tag:         "a76e1486",
			},
			Alias:      "Asset alias 0",
			Rolfp:      "R:0/O:1/L:0/F:1/P:0+S:1",
			Scannable:  true,
			AssetType:  (*AssetType)(strptr("Hostname")),
			Identifier: "www.example.com",
			Annotations: []*Annotation{
				{
					Key:   "annotation0/0",
					Value: "value0/0",
				},
				{
					Key:   "annotation0/1",
					Value: "value0/1",
				},
			},
		},
	},
	{
		Id: "9a86666e-ef3a-4630-845d-d3c61e167931/d2e37146-61d7-4010-aa25-2335c385a980",
		Payload: AssetPayload{
			Id: "d2e37146-61d7-4010-aa25-2335c385a980",
			Team: &Team{
				Id:          "9a86666e-ef3a-4630-845d-d3c61e167931",
				Name:        "Team name 1",
				Description: "Team description 1",
				Tag:         "f0eac043",
			},
			Alias:      "Asset alias 1",
			Rolfp:      "R:1/O:0/L:1/F:0/P:1+S:0",
			Scannable:  false,
			AssetType:  (*AssetType)(strptr("Hostname")),
			Identifier: "www.example.org",
			Annotations: []*Annotation{
				{
					Key:   "annotation1/0",
					Value: "value1/0",
				},
				{
					Key:   "annotation1/1",
					Value: "value1/1",
				},
			},
		},
	},
	{
		Id: "a86f4f99-a75c-436a-915d-905b825906d3/4ab22e34-889e-4c35-8ef2-4411bd162636",
		Payload: AssetPayload{
			Id: "4ab22e34-889e-4c35-8ef2-4411bd162636",
			Team: &Team{
				Id:          "a86f4f99-a75c-436a-915d-905b825906d3",
				Name:        "Team name 2",
				Description: "Team description 2",
				Tag:         "6deacae3",
			},
			Alias:      "Asset alias 2",
			Rolfp:      "R:1/O:1/L:1/F:1/P:1+S:1",
			Scannable:  true,
			AssetType:  (*AssetType)(strptr("DockerImage")),
			Identifier: "busybox:latest",
			Annotations: []*Annotation{
				{
					Key:   "annotation2/0",
					Value: "value2/0",
				},
				{
					Key:   "annotation2/1",
					Value: "value2/1",
				},
			},
		},
	},
	{
		Id: "32c3eb6a-189f-439c-b921-56ed27fa5c4a/4af3df0b-a2ca-46a4-bf5c-c35dbcb1d599",
		Payload: AssetPayload{
			Id: "4af3df0b-a2ca-46a4-bf5c-c35dbcb1d599",
			Team: &Team{
				Id:          "32c3eb6a-189f-439c-b921-56ed27fa5c4a",
				Name:        "Team name 3",
				Description: "Team description 3",
				Tag:         "bffc5223",
			},
			Alias:      "Asset alias 3",
			Rolfp:      "R:0/O:0/L:0/F:0/P:0+S:0",
			Scannable:  false,
			AssetType:  (*AssetType)(strptr("Hostname")),
			Identifier: "www.example.net",
			Annotations: []*Annotation{
				{
					Key:   "annotation3/0",
					Value: "value3/0",
				},
				{
					Key:   "annotation3/1",
					Value: "value3/1",
				},
			},
		},
	},
}

func TestClientProcessAssets(t *testing.T) {
	tests := []struct {
		name string
		msgs []streamtest.Message
		want []asset
	}{
		{
			name: "valid assets",
			msgs: streamtest.Parse("testdata/valid_assets.dat"),
			want: testdataValidAssets,
		},
		{
			name: "malformed assets",
			msgs: streamtest.Parse("testdata/malformed_assets.dat"),
			want: testdataValidAssets[:2],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := streamtest.NewMockProcessor(tt.msgs)
			cli := NewClient(mp)

			var got []asset
			cli.ProcessAssets(context.Background(), func(id string, payload AssetPayload) error {
				got = append(got, asset{id, payload})
				return nil
			})

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("asset mismatch (-want +got):\n%v", diff)
			}
		})
	}
}

func TestClientProcessAssetsError(t *testing.T) {
	// Number of assets to process before error.
	const n = 2

	if n > len(testdataValidAssets) {
		t.Fatal("n > testdata length")
	}

	mp := streamtest.NewMockProcessor(streamtest.Parse("testdata/valid_assets.dat"))
	cli := NewClient(mp)

	var (
		got []asset
		ctr int
	)
	cli.ProcessAssets(context.Background(), func(id string, payload AssetPayload) error {
		if ctr >= n {
			return errors.New("error")
		}

		got = append(got, asset{id, payload})
		ctr++

		return nil
	})

	if diff := cmp.Diff(testdataValidAssets[:n], got); diff != "" {
		t.Errorf("asset mismatch (-want +got):\n%v", diff)
	}
}

func strptr(s string) *string {
	return &s
}