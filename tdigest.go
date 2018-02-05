package tdigest

import (
	"container/heap"
	"math"
	"sort"
)

type TDigest struct {
	Compression float64

	maxProcessed      int
	maxUnprocessed    int
	processed         CentroidList
	unprocessed       CentroidList
	cumulative        []float64
	processedWeight   float64
	unprocessedWeight float64
	min               float64
	max               float64
}

func NewTDigest() *TDigest {
	t := &TDigest{
		Compression: 1000,
	}
	t.maxProcessed = processedSize(0, t.Compression)
	t.maxUnprocessed = processedSize(0, t.Compression)
	t.processed.Centroids = make([]*Centroid, 0, t.maxProcessed)
	t.unprocessed.Centroids = make([]*Centroid, 0, t.maxUnprocessed+1)
	t.max = math.MaxFloat64
	t.min = -math.MaxFloat64
	return t
}

/*
func (t *TDigest) Add(t *TDigest) error {
	// Heap this together with adds
	// there is some work around dealing with high water
	// updateCumulative as well
	return nil
}
*/

func (t *TDigest) AddCentroidList(c *CentroidList) {
	for i := range c.Centroids {
		diff := len(c.Centroids) - i
		room := t.maxUnprocessed - len(t.unprocessed.Centroids)
		mid := i + diff
		if room < diff {
			mid = i + room
		}
		for i < mid {
			t.AddCentroid(c.Centroids[i])
			i++
		}
	}
}

func (t *TDigest) AddCentroid(c *Centroid) {
	heap.Push(&t.unprocessed, c)
	t.unprocessedWeight += c.Weight

	if len(t.processed.Centroids) > t.maxProcessed ||
		len(t.unprocessed.Centroids) > t.maxUnprocessed {
		t.process()
	}
}

func (t *TDigest) process() {
	if len(t.unprocessed.Centroids) > 0 ||
		len(t.processed.Centroids) > t.maxProcessed {
		for i := range t.processed.Centroids {
			// I'm thinking that we should sort at the end
			// Right now this is O(N log N)
			heap.Push(&t.unprocessed, t.processed.Centroids[i])
		}
		t.processedWeight += t.unprocessedWeight
		t.unprocessedWeight = 0
		t.processed.Centroids = t.unprocessed.Centroids[:1]
		soFar := t.unprocessed.Centroids[0].Weight
		limit := t.processedWeight * t.integratedQ(1.0)
		for i := range t.unprocessed.Centroids {
			projected := soFar + t.unprocessed.Centroids[i].Weight
			if projected <= limit {
				soFar = projected
				heap.Push(&t.processed, t.unprocessed.Centroids[i])
			} else {
				k1 := t.integratedLocation(soFar / t.processedWeight)
				limit = t.processedWeight * t.integratedQ(k1+1.0)
				soFar = projected
				heap.Push(&t.processed, t.unprocessed.Centroids[i])
			}
		}
		// TODO: clear t.unprocessed
		t.min = math.Min(t.min, t.processed.Centroids[0].Mean)
		t.max = math.Max(t.max, t.processed.Centroids[len(t.processed.Centroids)-1].Mean)
		t.updateCumulative()
	}
}

func (t *TDigest) updateCumulative() {
	t.cumulative = make([]float64, 0, len(t.processed.Centroids)+1)
	prev := 0.0
	for i := range t.processed.Centroids {
		cur := t.processed.Centroids[i].Weight
		t.cumulative = append(t.cumulative, prev+cur/2.0)
		prev = prev + cur
	}
	t.cumulative[len(t.cumulative)-1] = prev
}

func (t *TDigest) Quantile(q float64) float64 {
	t.process()
	if q < 0 || q > 1 || len(t.processed.Centroids) == 0 {
		return math.NaN()
	}
	if len(t.processed.Centroids) == 1 {
		return 0.0
	}
	n := len(t.processed.Centroids)
	index := q * t.processedWeight
	if index < t.processed.Centroids[0].Weight/2.0 {
		return t.min + 2.0*index/t.processed.Centroids[0].Weight*(t.processed.Centroids[0].Mean-t.min)
	}

	lower := sort.Search(len(t.cumulative), func(i int) bool {
		return t.cumulative[i] < index
	})

	z1 := index - t.cumulative[lower-1]
	z2 := z1
	if lower != len(t.cumulative) {
		z2 = t.cumulative[lower] - index
	}
	return weightedAverage(t.processed.Centroids[n-1].Mean, z1, t.max, z2)
}

func (t *TDigest) CDF(x float64) float64 {
	t.process()
	switch len(t.processed.Centroids) {
	case 0:
		return 0.0
	case 1:
		width := t.max - t.min
		if x <= t.min {
			return 0.0
		}
		if x >= t.max {
			return 1.0
		}
		if (x - t.min) <= width {
			// min and max are too close together to do any viable interpolation
			return 0.5
		}
		return (x - t.min) / width
	}

	if x <= t.min {
		return 0.0
	}
	if x >= t.max {
		return 1.0
	}
	m0 := t.processed.Centroids[0].Mean
	// Left Tail
	if x <= m0 {
		if m0-t.min > 0 {
			return (x - t.min) / (m0 - t.min) * t.processed.Centroids[0].Weight / t.processedWeight / 2.0
		}
		return 0.0
	}
	// Right Tail
	mn := t.processed.Centroids[len(t.processed.Centroids)-1].Mean
	if x >= mn {
		if t.max-mn > 0.0 {
			return 1.0 - (t.max-x)/(t.max-mn)*t.processed.Centroids[len(t.processed.Centroids)-1].Weight/t.processedWeight/2.0
		}
		return 1.0
	}

	upper := sort.Search(len(t.processed.Centroids), func(i int) bool {
		return t.processed.Centroids[i].Mean > x
	})

	z1 := x - t.processed.Centroids[upper-1].Mean
	var z2 float64
	if upper == len(t.processed.Centroids) {
		z2 = z1
	} else {
		z2 = t.processed.Centroids[upper].Mean - x
	}
	return weightedAverage(t.cumulative[upper-1], z2, t.cumulative[upper], z1) / t.processedWeight
}

func (t *TDigest) integratedQ(k float64) float64 {
	return (math.Sin(math.Min(k, t.Compression)*math.Pi/t.Compression-math.Pi/2.0) + 1.0) / 2.0
}

func (t *TDigest) integratedLocation(q float64) float64 {
	return t.Compression * (math.Asin(2.0*q-1.0) + math.Pi/2.0) / math.Pi
}

func weightedAverage(x1, w1, x2, w2 float64) float64 {
	if x1 <= x2 {
		return weightedAverageSorted(x1, w1, x2, w2)
	}
	return weightedAverageSorted(x2, w2, x1, w1)
}

func weightedAverageSorted(x1, w1, x2, w2 float64) float64 {
	x := (x1*w1 + x2*w2) / (w1 + w2)
	return math.Max(x1, math.Min(x, x2))
}

func processedSize(size int, compression float64) int {
	if size == 0 {
		return int(2 * math.Ceil(compression))
	}
	return size
}

func unprocessedSize(size int, compression float64) int {
	if size == 0 {
		return int(8 * math.Ceil(compression))
	}
	return size
}
