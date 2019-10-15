package tdigest

import (
	"encoding/binary"
	"io"
)

// ExportToClickHouseQuantileTDigest serializes current TDigest state to ClickHouse compatible format.
// See https://github.com/ClickHouse/ClickHouse/blob/master/dbms/src/AggregateFunctions/QuantileTDigest.h
func ExportToClickHouseQuantileTDigest(td *TDigest, w io.Writer) error {
	td.process()

	cnt := td.processed.Len()

	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(cnt))
	if _, err := w.Write(tmp[:n]); err != nil {
		return err
	}

	for i := 0; i < cnt; i++ {
		centroid := &td.processed[i]
		if err := binary.Write(w, binary.LittleEndian, float32(centroid.Mean)); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, float32(centroid.Weight)); err != nil {
			return err
		}
	}

	return nil
}
