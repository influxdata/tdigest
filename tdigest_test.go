package tdigest_test

import (
	"math/rand"
	"testing"

	"github.com/gonum/stat/distuv"
	"github.com/influxdata/tdigest"
)

const (
	N     = 1e6
	Mu    = 10
	Sigma = 3

	seed = 42
)

// NormalData is a slice of N random values that are normaly distributed with mean Mu and standard deviation Sigma.
var NormalData []float64
var UniformData []float64

func init() {
	dist := distuv.Normal{
		Mu:     Mu,
		Sigma:  Sigma,
		Source: rand.New(rand.NewSource(seed)),
	}
	uniform := rand.New(rand.NewSource(seed))
	NormalData = make([]float64, N)
	UniformData = make([]float64, N)
	for i := range NormalData {
		NormalData[i] = dist.Rand()
		UniformData[i] = uniform.Float64() * 100
	}

}

func TestTdigest_Quantile(t *testing.T) {
	tests := []struct {
		name     string
		data     []float64
		quantile float64
		want     float64
	}{
		{
			name:     "increasing",
			quantile: 0.5,
			data:     []float64{1, 2, 3, 4, 5},
			want:     3,
		},
		{
			name:     "small",
			quantile: 0.5,
			data:     []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1},
			want:     3,
		},
		{
			name:     "normal 50",
			quantile: 0.5,
			data:     NormalData,
			want:     9.997821231634168,
		},
		{
			name:     "normal 90",
			quantile: 0.9,
			data:     NormalData,
			want:     13.843815760607427,
		},
		{
			name:     "uniform 50",
			quantile: 0.5,
			data:     UniformData,
			want:     50.02682856274754,
		},
		{
			name:     "uniform 90",
			quantile: 0.9,
			data:     UniformData,
			want:     90.02117754660424,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := tdigest.NewWithCompression(1000)
			for _, x := range tt.data {
				td.Add(x, 1)
			}
			got := td.Quantile(tt.quantile)
			if got != tt.want {
				t.Errorf("unexprected quantile %f, got %g want %g", tt.quantile, got, tt.want)
			}
		})
	}
}

var quantiles = []float64{0.1, 0.5, 0.9, 0.99, 0.999}

func BenchmarkTDigest_Add(b *testing.B) {
	for n := 0; n < b.N; n++ {
		td := tdigest.NewWithCompression(1000)
		for _, x := range NormalData {
			td.Add(x, 1)
		}
	}
}
func BenchmarkTDigest_Quantile(b *testing.B) {
	td := tdigest.NewWithCompression(1000)
	for _, x := range NormalData {
		td.Add(x, 1)
	}
	b.ResetTimer()
	var x float64
	for n := 0; n < b.N; n++ {
		for _, q := range quantiles {
			x += td.Quantile(q)
		}
	}
}
