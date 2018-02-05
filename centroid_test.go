package tdigest_test

import (
	"container/heap"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/tdigest"
)

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
				Weight: 1.0,
				Mean:   1.0,
			},
			r: tdigest.Centroid{
				Weight: 10.0,
				Mean:   12.0,
			},
			want: tdigest.Centroid{
				Weight: 11.0,
				Mean:   10.0,
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
			if !reflect.DeepEqual(tt.c, tt.want) {
				t.Errorf("centroid %+#v not equal to want %+#v", tt.c, tt.want)
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
			name: "priority should be by mean descending",
			centroids: []*tdigest.Centroid{
				&tdigest.Centroid{
					Mean: 1.0,
				},
				&tdigest.Centroid{
					Mean: 2.0,
				},
			},
			want: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean: 2.0,
					},
					&tdigest.Centroid{
						Mean: 1.0,
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
			opts := cmp.Options{
				cmpopts.IgnoreUnexported(tdigest.Centroid{}),
				cmpopts.IgnoreUnexported(tdigest.CentroidList{}),
			}
			if got := tdigest.NewCentroidList(tt.centroids); !cmp.Equal(tt.want, got, opts...) {
				t.Errorf("NewCentroidList() = -want/+got %s", cmp.Diff(tt.want, got, opts...))
			}
		})
	}
}

func TestCentroid_Pop(t *testing.T) {
	tests := []struct {
		name string
		list *tdigest.CentroidList
		want *tdigest.Centroid
	}{
		{
			name: "pop should remove centroid with greatest mean",
			list: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean: 2.0,
					},
					&tdigest.Centroid{
						Mean: 1.0,
					},
				},
			},
			want: &tdigest.Centroid{
				Mean: 2.0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := cmpopts.IgnoreUnexported(tdigest.Centroid{})
			if got := heap.Pop(tt.list); !cmp.Equal(tt.want, got, opt) {
				t.Errorf("CentroidList.Pop() = -want/+got %s", cmp.Diff(tt.want, got, opt))
			}
		})
	}
}

func TestCentroid_Push(t *testing.T) {
	tests := []struct {
		name string
		x    *tdigest.Centroid
		list *tdigest.CentroidList
		want *tdigest.CentroidList
	}{
		{
			name: "push with new larger mean should be at front",
			x: &tdigest.Centroid{
				Mean: 2.0,
			},
			list: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean: 1.0,
					},
				},
			},
			want: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean: 2.0,
					},
					&tdigest.Centroid{
						Mean: 1.0,
					},
				},
			},
		},
		{
			name: "push with new smaller mean should be at back",
			x: &tdigest.Centroid{
				Mean: 2.0,
			},
			list: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean: 3.0,
					},
				},
			},
			want: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean: 3.0,
					},
					&tdigest.Centroid{
						Mean: 2.0,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := cmp.Options{
				cmpopts.IgnoreUnexported(tdigest.Centroid{}),
				cmpopts.IgnoreUnexported(tdigest.CentroidList{}),
			}
			if heap.Push(tt.list, tt.x); !cmp.Equal(tt.want, tt.list, opts...) {
				t.Errorf("CentroidList.Push() = -want/+got %s", cmp.Diff(tt.want, tt.list, opts...))
			}
		})
	}
}

func TestCentroid_Update(t *testing.T) {
	tests := []struct {
		name   string
		mean   float64
		weight float64
		list   *tdigest.CentroidList
		want   *tdigest.CentroidList
	}{
		{
			name:   "push with new larger mean should be at front",
			mean:   5.0,
			weight: 7.0,
			list: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean: 2.0,
					},
					&tdigest.Centroid{
						Mean: 1.0,
					},
				},
			},
			want: &tdigest.CentroidList{
				Centroids: []*tdigest.Centroid{
					&tdigest.Centroid{
						Mean:   5.0,
						Weight: 7.0,
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
			opts := cmp.Options{
				cmpopts.IgnoreUnexported(tdigest.Centroid{}),
				cmpopts.IgnoreUnexported(tdigest.CentroidList{}),
			}
			if tt.list.Update(tt.list.Centroids[0], tt.mean, tt.weight); !cmp.Equal(tt.want, tt.list, opts...) {
				t.Errorf("CentroidList.Update() = -want/+got %s", cmp.Diff(tt.want, tt.list, opts...))
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
			opts := cmp.Options{
				cmpopts.IgnoreUnexported(tdigest.Centroid{}),
				cmpopts.IgnoreUnexported(tdigest.CentroidList{}),
			}
			if got := tt.list.Weight(); !cmp.Equal(tt.want, got, opts...) {
				t.Errorf("CentroidList.Weight() = -want/+got %s", cmp.Diff(tt.want, got, opts...))
			}
		})
	}
}
