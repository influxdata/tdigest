package tdigest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const (
	magic           = int16(0xc80)
	encodingVersion = int32(1)
)

func marshalBinary(d *TDigest) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	w := &binaryBufferWriter{buf: buf}
	w.writeValue(magic)
	w.writeValue(encodingVersion)
	w.writeValue(d.Compression)
	w.writeValue(int32(len(d.processed)))
	for _, c := range d.processed {
		w.writeValue(c.Weight)
		w.writeValue(c.Mean)
	}
	w.writeValue(int32(len(d.cumulative)))
	for _, c := range d.cumulative {
		w.writeValue(c)
	}
	w.writeValue(d.decayCount)
	w.writeValue(d.decayEvery)
	w.writeValue(d.decayValue)
	w.writeValue(d.count)
	w.writeValue(d.min)
	w.writeValue(d.max)

	if w.err != nil {
		return nil, w.err
	}
	return buf.Bytes(), nil
}

func unmarshalBinary(d *TDigest, p []byte) error {
	var (
		mv int16
		ev int32
		n  int32
	)
	r := &binaryReader{r: bytes.NewReader(p)}
	r.readValue(&mv)
	if r.err != nil {
		return r.err
	}
	if mv != magic {
		return fmt.Errorf("data corruption detected: invalid header magic value 0x%04x", mv)
	}
	r.readValue(&ev)
	if r.err != nil {
		return r.err
	}
	if ev != encodingVersion {
		return fmt.Errorf("data corruption detected: invalid encoding version %d", ev)
	}
	r.readValue(&d.Compression)
	d.maxProcessed = processedSize(0, d.Compression)
	d.maxUnprocessed = unprocessedSize(0, d.Compression)
	d.processed = make([]Centroid, 0, d.maxProcessed)
	d.unprocessed = make([]Centroid, 0, d.maxUnprocessed+1)
	d.cumulative = make([]float64, 0, d.maxProcessed+1)
	r.readValue(&n)
	if r.err != nil {
		return r.err
	}
	if n < 0 {
		return fmt.Errorf("data corruption detected: number of centroids cannot be negative, have %v", n)

	}
	if n > 1<<20 {
		return fmt.Errorf("invalid n, cannot be greater than 2^20: %v", n)
	}
	for i := 0; i < int(n); i++ {
		c := Centroid{}
		r.readValue(&c.Weight)
		r.readValue(&c.Mean)
		if r.err != nil {
			return r.err
		}
		if c.Weight < 0 {
			return fmt.Errorf("data corruption detected: negative count: %f", c.Weight)
		}
		if math.IsNaN(c.Mean) {
			return fmt.Errorf("data corruption detected: NaN mean not permitted")
		}
		if math.IsInf(c.Mean, 0) {
			return fmt.Errorf("data corruption detected: Inf mean not permitted")
		}
		if i > 0 {
			prev := d.processed[i-1]
			if c.Mean < prev.Mean {
				return fmt.Errorf("data corruption detected: centroid %d has lower mean (%v) than preceding centroid %d (%v)", i, c.Mean, i-1, prev.Mean)
			}
		}
		d.processed = append(d.processed, c)
		if c.Weight > math.MaxInt64-d.processedWeight {
			return fmt.Errorf("data corruption detected: centroid total size overflow")
		}
		d.processedWeight += c.Weight
	}

	r.readValue(&n)
	if r.err != nil {
		return r.err
	}
	if n < 0 {
		return fmt.Errorf("data corruption detected: number of cumulatives cannot be negative, have %v", n)
	}
	if n > 1<<20 {
		return fmt.Errorf("invalid n, cannot be greater than 2^20: %v", n)
	}

	for i := 0; i < int(n); i++ {
		var v float64
		r.readValue(&v)
		if math.IsNaN(v) {
			return fmt.Errorf("data corruption detected: NaN mean not permitted")
		}
		if math.IsInf(v, 0) {
			return fmt.Errorf("data corruption detected: Inf mean not permitted")
		}
		d.cumulative = append(d.cumulative, v)
	}

	r.readValue(&d.decayCount)
	if r.err != nil {
		return r.err
	}
	r.readValue(&d.decayEvery)
	if r.err != nil {
		return r.err
	}
	r.readValue(&d.decayValue)
	if r.err != nil {
		return r.err
	}
	r.readValue(&d.count)
	if r.err != nil {
		return r.err
	}
	r.readValue(&d.min)
	if r.err != nil {
		return r.err
	}
	r.readValue(&d.max)
	if r.err != nil {
		return r.err
	}

	if n := r.r.Len(); n > 0 {
		return fmt.Errorf("found %d unexpected bytes trailing the tdigest", n)
	}

	return nil
}

type binaryBufferWriter struct {
	buf *bytes.Buffer
	err error
}

func (w *binaryBufferWriter) writeValue(v interface{}) {
	if w.err != nil {
		return
	}
	w.err = binary.Write(w.buf, binary.LittleEndian, v)
}

type binaryReader struct {
	r   *bytes.Reader
	err error
}

func (r *binaryReader) readValue(v interface{}) {
	if r.err != nil {
		return
	}
	r.err = binary.Read(r.r, binary.LittleEndian, v)
	if r.err == io.EOF {
		r.err = io.ErrUnexpectedEOF
	}
}
