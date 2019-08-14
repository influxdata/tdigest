package tdigest

import (
	"testing"

	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
	"math"
	"reflect"
	"time"
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

var NormalDigest *TDigest
var UniformDigest *TDigest

func init() {
	dist := distuv.Normal{
		Mu:    Mu,
		Sigma: Sigma,
		Src:   rand.New(rand.NewSource(seed)),
	}
	uniform := rand.New(rand.NewSource(seed))

	UniformData = make([]float64, N)
	UniformDigest = NewWithCompression(1000)

	NormalData = make([]float64, N)
	NormalDigest = NewWithCompression(1000)

	for i := range NormalData {
		NormalData[i] = dist.Rand()
		NormalDigest.Add(NormalData[i], 1)

		UniformData[i] = uniform.Float64() * 100
		UniformDigest.Add(UniformData[i], 1)
	}
}

func TestTdigest_Quantile(t *testing.T) {
	tests := []struct {
		name     string
		data     []float64
		digest   *TDigest
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
			name:     "data in decreasing order",
			quantile: 0.25,
			data:     []float64{555.349107, 432.842597},
			want:     432.842597,
		},
		{
			name:     "small",
			quantile: 0.5,
			data:     []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1},
			want:     3,
		},
		{
			name:     "small 99 (max)",
			quantile: 0.99,
			data:     []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1},
			want:     5,
		},
		{
			name:     "normal 50",
			quantile: 0.5,
			digest:   NormalDigest,
			want:     10.000673533707138,
		},
		{
			name:     "normal 90",
			quantile: 0.9,
			digest:   NormalDigest,
			want:     13.842132136909889,
		},
		{
			name:     "uniform 50",
			quantile: 0.5,
			digest:   UniformDigest,
			want:     49.992502345843555,
		},
		{
			name:     "uniform 90",
			quantile: 0.9,
			digest:   UniformDigest,
			want:     89.98281777095822,
		},
		{
			name:     "uniform 99",
			quantile: 0.99,
			digest:   UniformDigest,
			want:     98.98503400959562,
		},
		{
			name:     "uniform 99.9",
			quantile: 0.999,
			digest:   UniformDigest,
			want:     99.90103781043621,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := tt.digest
			if td == nil {
				td = NewWithCompression(1000)
				for _, x := range tt.data {
					td.Add(x, 1)
				}
			}
			got := td.Quantile(tt.quantile)
			if got != tt.want {
				t.Errorf("unexpected quantile %f, got %g want %g", tt.quantile, got, tt.want)
			}
		})
	}
}

func TestClone(t *testing.T) {
	testcase := func(in *TDigest) func(*testing.T) {
		return func(t *testing.T) {
			b, err := in.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary err: %v", err)
			}
			out := new(TDigest)
			err = out.UnmarshalBinary(b)
			if err != nil {
				t.Fatalf("UnmarshalBinary err: %v", err)
			}
			if !reflect.DeepEqual(in, out) {
				t.Errorf("marshaling round trip resulted in changes")
				t.Logf("in: %+v", in)
				t.Logf("out: %+v", out)
			}
		}
	}
	t.Run("empty", testcase(New()))
	t.Run("1 value", testcase(simpleTDigest(1)))
	t.Run("1000 values", testcase(simpleTDigest(1000)))

	d := New()
	d.Add(1, 1)
	d.Add(1, 1)
	d.Add(0, 1)
	t.Run("1, 1, 0 input", testcase(d))
}

func TestTdigest_CDFs(t *testing.T) {
	tests := []struct {
		name   string
		data   []float64
		digest *TDigest
		cdf    float64
		want   float64
	}{
		{
			name: "increasing",
			cdf:  3,
			data: []float64{1, 2, 3, 4, 5},
			want: 0.5,
		},
		{
			name: "small",
			cdf:  4,
			data: []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1},
			want: 0.75,
		},
		{
			name: "small max",
			cdf:  5,
			data: []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1},
			want: 1,
		},
		{
			name: "normal mean",
			cdf:  10,
			data: NormalData,
			want: 0.4999156505250766,
		},
		{
			name: "normal high",
			cdf:  -100,
			data: NormalData,
			want: 0,
		},
		{
			name: "normal low",
			cdf:  110,
			data: NormalData,
			want: 1,
		},
		{
			name: "uniform 50",
			cdf:  50,
			data: UniformData,
			want: 0.5000756133965755,
		},
		{
			name: "uniform min",
			cdf:  0,
			data: UniformData,
			want: 0,
		},
		{
			name: "uniform max",
			cdf:  100,
			data: UniformData,
			want: 1,
		},
		{
			name: "uniform 10",
			cdf:  10,
			data: UniformData,
			want: 0.09987932577650871,
		},
		{
			name: "uniform 90",
			cdf:  90,
			data: UniformData,
			want: 0.9001667885256108,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := tt.digest
			if td == nil {
				td = NewWithCompression(1000)
				for _, x := range tt.data {
					td.Add(x, 1)
				}
			}
			got := td.CDF(tt.cdf)
			if got != tt.want {
				t.Errorf("unexpected CDF %f, got %g want %g", tt.cdf, got, tt.want)
			}
		})
	}
}

func TestCloneRoundTrip(t *testing.T) {
	testcase := func(in *TDigest) func(*testing.T) {
		return func(t *testing.T) {

			out := in.Clone()
			if !reflect.DeepEqual(in, out) {
				t.Errorf("marshaling round trip resulted in changes")
				t.Logf("inn: %+v", in)
				t.Logf("out: %+v", out)
			}
		}
	}
	t.Run("empty", testcase(New()))
	t.Run("1 value", testcase(simpleTDigest(1)))
	t.Run("1000 values", testcase(simpleTDigest(1000)))

	d := New()
	d.Add(1, 1)
	d.Add(1, 1)
	d.Add(0, 1)
	t.Run("1, 1, 0 input", testcase(d))
}

var (
	quantiles            = []float64{0.1, 0.5, 0.9, 0.99, 0.999}
	benchmarkCompression = float64(500)
	benchmarkDecayValue  = 0.9
	benchmarkDecayEvery  = int32(1000)
)

func BenchmarkAdd(b *testing.B) {
	rand.Seed(uint64(time.Now().Unix()))
	benchmarks := []struct {
		name  string
		scale scaler
	}{
		{name: "k1", scale: &K1{}},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			td := NewWithDecay(benchmarkCompression, benchmarkDecayValue, benchmarkDecayEvery)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				td.Add(math.Abs(rand.NormFloat64()), 1.0)
			}
		})
	}
}

func BenchmarkQuantile(b *testing.B) {
	rand.Seed(uint64(time.Now().Unix()))
	benchmarks := []struct {
		name  string
		scale scaler
	}{
		{name: "k1", scale: &K1{}},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			td := NewWithDecay(benchmarkCompression, benchmarkDecayValue, benchmarkDecayEvery)
			for i := 0; i < b.N; i++ {
				td.Add(math.Abs(rand.NormFloat64()), 1.0)
			}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				td.Quantile(rand.Float64())
			}
		})
	}
}

func BenchmarkCDF(b *testing.B) {
	rand.Seed(uint64(time.Now().Unix()))
	benchmarks := []struct {
		name  string
		scale scaler
	}{
		{name: "k1", scale: &K1{}},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			td := NewWithDecay(benchmarkCompression, benchmarkDecayValue, benchmarkDecayEvery)
			for i := 0; i < b.N; i++ {
				td.Add(math.Abs(rand.NormFloat64()), 1.0)
			}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				td.CDF(math.Abs(rand.NormFloat64()))
			}
		})
	}
}
