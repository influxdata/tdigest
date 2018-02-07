package tdigest_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/tdigest"
)

var CmpOptions = cmp.Options{
	cmpopts.IgnoreUnexported(tdigest.Centroid{}),
	cmpopts.IgnoreUnexported(tdigest.CentroidList{}),
}

func TestCentroid_Add(t *testing.T) {
	tests := []struct {
		name    string
		c       tdigest.Centroid
		r       tdigest.Centroid
		want    tdigest.Centroid
		wantErr bool
		errStr  string
	}{
		{
			name: "error when weight is zero",
			r: tdigest.Centroid{
				Weight: -1.0,
			},
			wantErr: true,
			errStr:  "centroid weight cannot be less than zero",
		},
		{
			name: "zero weight",
			c: tdigest.Centroid{
				Weight: 0.0,
				Mean:   1.0,
			},
			r: tdigest.Centroid{
				Weight: 1.0,
				Mean:   2.0,
			},
			want: tdigest.Centroid{
				Weight: 1.0,
				Mean:   2.0,
			},
		},
		{
			name: "weight order of magnitude",
			c: tdigest.Centroid{
				Weight: 1,
				Mean:   1,
			},
			r: tdigest.Centroid{
				Weight: 10,
				Mean:   10,
			},
			want: tdigest.Centroid{
				Weight: 11,
				Mean:   9.181818181818182,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &tt.c
			if err := c.Add(&tt.r); (err != nil) != tt.wantErr {
				t.Errorf("Centroid.Add() error = %v, wantErr %v", err, tt.wantErr)
			} else if tt.wantErr && err.Error() != tt.errStr {
				t.Errorf("Centroid.Add() error.Error() = %s, errStr %v", err.Error(), tt.errStr)
			}
			if !cmp.Equal(tt.c, tt.want, CmpOptions...) {
				t.Errorf("unexprected centroid -want/+got\n%s", cmp.Diff(tt.want, tt.c, CmpOptions...))
			}
		})
	}
}

func TestNewCentroidList(t *testing.T) {
	tests := []struct {
		name      string
		centroids []*tdigest.Centroid
		want      *tdigest.CentroidList
	}{
		{
			name: "empty list",
			want: &tdigest.CentroidList{},
		},
		{
			name: "priority should be by mean ascending",
			centroids: []*tdigest.Centroid{
				&tdigest.Centroid{
					Mean: 2.0,
				},
				&tdigest.Centroid{
					Mean: 1.0,
				},
			},
			want: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean: 1.0,
					},
					&tdigest.Centroid{
						Mean: 2.0,
					},
				},
			},
		},
		{
			name: "single element should be identity",
			centroids: []*tdigest.Centroid{
				&tdigest.Centroid{
					Mean: 1.0,
				},
			},
			want: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean: 1.0,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tdigest.NewCentroidList(tt.centroids); !cmp.Equal(tt.want, got, CmpOptions...) {
				t.Errorf("NewCentroidList() = -want/+got %s", cmp.Diff(tt.want, got, CmpOptions...))
			}
		})
	}
}

func TestCentroid_Weight(t *testing.T) {
	tests := []struct {
		name string
		list *tdigest.CentroidList
		want float64
	}{
		{
			name: "should be the sum total of all centroid weights",
			list: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean:   2.0,
						Weight: 2.0,
					},
					&tdigest.Centroid{
						Mean:   1.0,
						Weight: 1.0,
					},
				},
			},
			want: 3.0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.list.Weight(); !cmp.Equal(tt.want, got, CmpOptions...) {
				t.Errorf("CentroidList.Weight() = -want/+got %s", cmp.Diff(tt.want, got, CmpOptions...))
			}
		})
	}
}
