package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/influxdata/tdigest"
)

var quantiles = []float64{
	0.1,
	0.2,
	0.5,
	0.75,
	0.9,
	0.99,
	0.999,
}

var dataFiles = []string{
	"small.dat",
	"uniform.dat",
	"normal.dat",
}

func main() {
	for _, f := range dataFiles {
		data := loadData(f)
		results := computeQuantiles(data, quantiles)
		writeResults(f+".go.quantiles", results, quantiles)
	}
}

func loadData(name string) []float64 {
	f, err := os.Open(name)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	var data []float64
	for s.Scan() {
		x, err := strconv.ParseFloat(s.Text(), 64)
		if err != nil {
			panic(err)
		}
		data = append(data, x)
	}
	return data
}

func computeQuantiles(data, quantiles []float64) (r []float64) {
	td := tdigest.NewWithCompression(1000)
	for _, x := range data {
		td.Add(x, 1)
	}
	for _, q := range quantiles {
		r = append(r, td.Quantile(q))
	}
	return
}

func writeResults(name string, results, quantiles []float64) {
	f, err := os.Create(name)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for i := range results {
		fmt.Fprintf(f, "%.20f %.20f\n", results[i], quantiles[i])
	}
}
