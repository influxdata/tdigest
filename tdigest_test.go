package tdigest_test

import (
	"testing"

	"github.com/influxdata/tdigest"
)

func TestTdigest(t *testing.T) {
	tests := []struct {
		name string
		list *tdigest.CentroidList
		want float64
	}{
		{
			name: "test quantile",
			list: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean: 20.0,
					},
					&tdigest.Centroid{
						Mean: 2.0,
					},
					&tdigest.Centroid{
						Mean: 1.0,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := tdigest.NewTDigest()
			td.AddCentroidList(tt.list)
			got := td.Quantile(0.5)
			if got != tt.want {
				t.Errorf("quantile %f not equal to want %f", got, tt.want)
			}
		})
	}
}
