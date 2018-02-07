package main

import (
	"bufio"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

var dataFiles = []string{
	"small.dat",
	"uniform.dat",
	"normal.dat",
}

const (
	cppExt = ".cpp.quantiles"
	goExt  = ".go.quantiles"

	epsilon = 1e-6
)

func main() {
	for _, f := range dataFiles {
		cppQuantiles := loadQuantiles(f + cppExt)
		goQuantiles := loadQuantiles(f + goExt)
		if len(cppQuantiles) != len(goQuantiles) {
			log.Fatal("differing number of quantiles")
		}

		for i := range cppQuantiles {
			if math.Abs(cppQuantiles[i]-goQuantiles[i]) > epsilon {
				log.Fatalf("differing quantile result go: %f cpp: %f", goQuantiles[i], cppQuantiles[i])
			}
		}
	}
}
func loadQuantiles(name string) []float64 {
	f, err := os.Open(name)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	var data []float64
	for s.Scan() {
		parts := strings.SplitN(s.Text(), " ", 2)
		x, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			panic(err)
		}
		data = append(data, x)
	}
	return data
}
