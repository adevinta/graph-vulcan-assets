package vulcan

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/adevinta/graph-vulcan-assets/stream"
	"github.com/adevinta/graph-vulcan-assets/stream/streamtest"
)

type asset struct {
	Payload AssetPayload
	IsNil   bool
}

var testdataValidAssets = []asset{
	{
		Payload: AssetPayload{
			ID: "f110cf6f-803d-442c-9b42-f6d8cd962bf2",
			Team: Team{
				ID:          "9a1a0332-88b6-4edc-aa37-50adc1ad96da",
				Name:        "Team name 0",
				Description: "Team description 0",
				Tag:         "a76e1486",
			},
			Alias:      "Asset alias 0",
			Rolfp:      "R:0/O:1/L:0/F:1/P:0+S:1",
			Scannable:  true,
			AssetType:  AssetType("Hostname"),
			Identifier: "www.example.com",
			Annotations: []Annotation{
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
		IsNil: false,
	},
	{
		Payload: AssetPayload{
			ID: "d2e37146-61d7-4010-aa25-2335c385a980",
			Team: Team{
				ID:          "9a86666e-ef3a-4630-845d-d3c61e167931",
				Name:        "Team name 1",
				Description: "Team description 1",
				Tag:         "f0eac043",
			},
			Alias:      "Asset alias 1",
			Rolfp:      "R:1/O:0/L:1/F:0/P:1+S:0",
			Scannable:  false,
			AssetType:  AssetType("Hostname"),
			Identifier: "www.example.org",
			Annotations: []Annotation{
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
		IsNil: false,
	},
	{
		Payload: AssetPayload{
			ID: "4ab22e34-889e-4c35-8ef2-4411bd162636",
			Team: Team{
				ID:          "a86f4f99-a75c-436a-915d-905b825906d3",
				Name:        "Team name 2",
				Description: "Team description 2",
				Tag:         "6deacae3",
			},
			Alias:      "Asset alias 2",
			Rolfp:      "R:1/O:1/L:1/F:1/P:1+S:1",
			Scannable:  true,
			AssetType:  AssetType("DockerImage"),
			Identifier: "busybox:latest",
			Annotations: []Annotation{
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
		IsNil: false,
	},
	{
		Payload: AssetPayload{
			ID: "4af3df0b-a2ca-46a4-bf5c-c35dbcb1d599",
			Team: Team{
				ID:          "32c3eb6a-189f-439c-b921-56ed27fa5c4a",
				Name:        "Team name 3",
				Description: "Team description 3",
				Tag:         "bffc5223",
			},
			Alias:      "Asset alias 3",
			Rolfp:      "R:0/O:0/L:0/F:0/P:0+S:0",
			Scannable:  false,
			AssetType:  AssetType("Hostname"),
			Identifier: "www.example.net",
			Annotations: []Annotation{
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
		IsNil: false,
	},
	{
		Payload: AssetPayload{
			ID:         "15ae9294-e1ed-4615-8423-2b78e5d04b95",
			AssetType:  AssetType("DockerImage"),
			Identifier: "nilvalue:latest",
			Team: Team{
				ID: "bdb2e4a3-5d86-46f8-aae0-b2cd3a56e230",
			},
		},
		IsNil: true,
	},
}

func TestClientProcessAssets(t *testing.T) {
	tests := []struct {
		name       string
		msgs       []stream.Message
		wantAssets []asset
		wantNilErr bool
	}{
		{
			name:       "valid assets",
			msgs:       streamtest.MustParse("testdata/valid_assets.json"),
			wantAssets: testdataValidAssets,
			wantNilErr: true,
		},
		{
			name:       "malformed assets",
			msgs:       streamtest.MustParse("testdata/malformed_assets.json"),
			wantAssets: testdataValidAssets[:2],
			wantNilErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := streamtest.NewMockProcessor(tt.msgs)
			cli := NewClient(mp)

			var got []asset
			err := cli.ProcessAssets(context.Background(), func(payload AssetPayload, isNil bool) error {
				got = append(got, asset{payload, isNil})
				return nil
			})

			if (err == nil) != tt.wantNilErr {
				t.Errorf("unexpected error: wantNilErr=%v got=%v", tt.wantNilErr, err)
			}

			if diff := cmp.Diff(tt.wantAssets, got); diff != "" {
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

	mp := streamtest.NewMockProcessor(streamtest.MustParse("testdata/valid_assets.json"))
	cli := NewClient(mp)

	wantErr := errors.New("error")

	var (
		got []asset
		ctr int
	)
	err := cli.ProcessAssets(context.Background(), func(payload AssetPayload, isNil bool) error {
		if ctr >= n {
			return wantErr
		}

		got = append(got, asset{payload, isNil})
		ctr++

		return nil
	})

	if err != wantErr {
		t.Errorf("error mismatch: want=%v got=%v", wantErr, err)
	}

	if diff := cmp.Diff(testdataValidAssets[:n], got); diff != "" {
		t.Errorf("asset mismatch (-want +got):\n%v", diff)
	}
}

func TestSupportedVersion(t *testing.T) {
	tests := []struct {
		name string
		v    string
		want bool
	}{
		{
			name: "supported version starting with v",
			v:    "v0.2.3",
			want: true,
		},
		{
			name: "supported version",
			v:    "0.2.3",
			want: true,
		},
		{
			name: "supported version with leading zero",
			v:    "00.2.3",
			want: true,
		},
		{
			name: "unsupported version",
			v:    "11.2.3",
			want: false,
		},
		{
			name: "malformed version",
			v:    "0.2",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := supportedVersion(tt.v)
			if got != tt.want {
				t.Errorf("unexpected result: v=%v got=%v want=%v", tt.v, got, tt.want)
			}
		})
	}
}
