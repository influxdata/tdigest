package tdigest

import "container/heap"

// ErrWeightLessThanZero is used when the weight is not able to be processed.
const ErrWeightLessThanZero = Error("centroid weight cannot be less than zero")

// Error is a domain error encountered while processing tdigests
type Error string

func (e Error) Error() string {
	return string(e)
}

// Centroid average position of all points in a shape
type Centroid struct {
	Mean   float64
	Weight float64
	index  int
}

// Add averages the two centroids together and update this centroid
func (c *Centroid) Add(r *Centroid) error {
	if r.Weight < 0 {
		return ErrWeightLessThanZero
	}
	if c.Weight != 0 {
		c.Weight += r.Weight
		c.Mean = r.Weight * (r.Mean - c.Mean) / c.Weight
	} else {
		c.Weight = r.Weight
		c.Mean = r.Mean
	}
	return nil
}

// CentroidList is a priority queue sorted by the Mean of the centroid, descending.
type CentroidList struct {
	Centroids []*Centroid
	index     int
}

// Weight returns the summed weight of all centroids
func (l *CentroidList) Weight() (w float64) {
	for i := range l.Centroids {
		w += l.Centroids[i].Weight
	}
	return w
}

func (l *CentroidList) Len() int { return len(l.Centroids) }

func (l *CentroidList) Less(i, j int) bool {
	return l.Centroids[i].Mean > l.Centroids[j].Mean
}

func (l *CentroidList) Swap(i, j int) {
	l.Centroids[i], l.Centroids[j] = l.Centroids[j], l.Centroids[i]
	l.Centroids[i].index = i
	l.Centroids[j].index = j
}

// Push pushes the centroid x onto the CentroidList priority queue
func (l *CentroidList) Push(x interface{}) {
	n := len(l.Centroids)
	item := x.(*Centroid)
	item.index = n
	l.Centroids = append(l.Centroids, item)
}

// Pop removes the centroid with the maximum mean from the priority queue
func (l *CentroidList) Pop() interface{} {
	old := l.Centroids
	n := len(old)
	item := old[n-1]
	item.index = -1
	l.Centroids = old[0 : n-1]
	return item
}

// Update changes the mean and weights of a centroid and reprioritizes the priority queue
func (l *CentroidList) Update(c *Centroid, mean, weight float64) {
	c.Mean = mean
	c.Weight = weight
	heap.Fix(l, c.index)
}

// NewCentroidList creates a priority queue for the centroids
func NewCentroidList(centroids []*Centroid) *CentroidList {
	l := &CentroidList{
		Centroids: centroids,
	}
	heap.Init(l)
	return l
}
