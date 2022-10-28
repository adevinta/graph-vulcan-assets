package streamtest

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		want        []Message
		shouldPanic bool
	}{
		{
			name:     "valid file",
			filename: "testdata/valid.dat",
			want: []Message{
				{
					Key:   []byte("key0"),
					Value: []byte("value0"),
				},
				{
					Key:   []byte("key1"),
					Value: []byte("value1:a:b:c"),
				},
				{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				},
			},
			shouldPanic: false,
		},
		{
			name:        "malformed file",
			filename:    "testdata/malformed.dat",
			want:        nil,
			shouldPanic: true,
		},
		{
			name:        "nonexistent file",
			filename:    "testdata/nonexistent.dat",
			want:        nil,
			shouldPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if err := recover(); (err != nil) != tt.shouldPanic {
					t.Errorf("unexpected panic behavior")
				}
			}()

			got := Parse(tt.filename)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("asset mismatch (-want +got):\n%v", diff)
			}
		})
	}
}
