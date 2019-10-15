package tdigest_test

import (
	"bytes"
	"fmt"
	"github.com/influxdata/tdigest"
	"testing"
)

func TestExportToClickHouseQuantileTDigest(t *testing.T) {
	const compression = 1000

	tests := []struct {
		count int
		want  []byte
	}{
		{
			count: 0,
			want:  []byte{0},
		},
		{
			count: 1,
			want:  []byte{1, 0, 0, 0, 0, 0, 0, 128, 63},
		},
		{
			count: 20,
			want:  []byte{20, 0, 0, 0, 0, 0, 0, 128, 63, 0, 0, 128, 63, 0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 128, 63, 0, 0, 64, 64, 0, 0, 128, 63, 0, 0, 128, 64, 0, 0, 128, 63, 0, 0, 160, 64, 0, 0, 128, 63, 0, 0, 192, 64, 0, 0, 128, 63, 0, 0, 224, 64, 0, 0, 128, 63, 0, 0, 0, 65, 0, 0, 128, 63, 0, 0, 16, 65, 0, 0, 128, 63, 0, 0, 32, 65, 0, 0, 128, 63, 0, 0, 48, 65, 0, 0, 128, 63, 0, 0, 64, 65, 0, 0, 128, 63, 0, 0, 80, 65, 0, 0, 128, 63, 0, 0, 96, 65, 0, 0, 128, 63, 0, 0, 112, 65, 0, 0, 128, 63, 0, 0, 128, 65, 0, 0, 128, 63, 0, 0, 136, 65, 0, 0, 128, 63, 0, 0, 144, 65, 0, 0, 128, 63, 0, 0, 152, 65, 0, 0, 128, 63},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf(`%d elements`, tt.count), func(t *testing.T) {
			td := tdigest.NewWithCompression(compression)
			for i := 0; i < tt.count; i++ {
				td.Add(float64(i), 1)
			}

			var buf bytes.Buffer
			err := tdigest.ExportToClickHouseQuantileTDigest(td, &buf)
			if err != nil {
				t.Errorf("an error has occurred during serialization: %s", err)
			} else if got := buf.Bytes(); !bytes.Equal(tt.want, got) {
				t.Errorf("wrong serialized state, got %+v want %+v", got, tt.want)
			}
		})
	}
}
