package streamtest

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/adevinta/graph-vulcan-assets/stream"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		want        []stream.Message
		shouldPanic bool
	}{
		{
			name:     "valid file",
			filename: "testdata/valid.json",
			want: []stream.Message{
				{
					Key:   []byte("key0"),
					Value: []byte("value0"),
					Metadata: []stream.MetadataEntry{
						{
							Key:   []byte("hkey00"),
							Value: []byte("hvalue00"),
						},
						{
							Key:   []byte("hkey01"),
							Value: []byte("hvalue01"),
						},
					},
				},
				{
					Key:      []byte("key1"),
					Value:    []byte("value1"),
					Metadata: nil,
				},
				{
					Key:      []byte("key2"),
					Value:    nil,
					Metadata: nil,
				},
				{
					Key:      nil,
					Value:    []byte("value3"),
					Metadata: nil,
				},
			},
			shouldPanic: false,
		},
		{
			name:        "malformed json",
			filename:    "testdata/malformed_json.json",
			want:        nil,
			shouldPanic: true,
		},
		{
			name:        "null metadata key",
			filename:    "testdata/malformed_null_metadata_key.json",
			want:        nil,
			shouldPanic: true,
		},
		{
			name:        "null metadata value",
			filename:    "testdata/malformed_null_metadata_value.json",
			want:        nil,
			shouldPanic: true,
		},
		{
			name:        "nonexistent file",
			filename:    "testdata/nonexistent.json",
			want:        nil,
			shouldPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if err := recover(); (err != nil) != tt.shouldPanic {
					t.Errorf("unexpected panic behavior: %v", err)
				}
			}()

			got := MustParse(tt.filename)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("asset mismatch (-want +got):\n%v", diff)
			}
		})
	}
}
