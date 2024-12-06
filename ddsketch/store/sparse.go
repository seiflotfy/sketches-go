// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package store

import (
	"errors"
	"sort"

	enc "github.com/DataDog/sketches-go/ddsketch/encoding"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"

	"github.com/kamstrup/intmap"
)

type SparseStore struct {
	counts *intmap.Map[int, float64]
}

func NewSparseStore() *SparseStore {
	return &SparseStore{counts: intmap.New[int, float64](0)}
}

func (s *SparseStore) Add(index int) {
	val, exists := s.counts.Get(index)
	if exists {
		s.counts.Put(index, val+1)
	} else {
		s.counts.Put(index, 1)
	}
}

func (s *SparseStore) AddBin(bin Bin) {
	s.AddWithCount(bin.index, bin.count)
}

func (s *SparseStore) AddWithCount(index int, count float64) {
	if count == 0 {
		return
	}
	val, exists := s.counts.Get(index)
	if exists {
		s.counts.Put(index, val+count)
	} else {
		s.counts.Put(index, count)
	}
}

func (s *SparseStore) Bins() <-chan Bin {
	ch := make(chan Bin)
	go func() {
		defer close(ch)
		orderedBins := s.orderedBins()
		for _, bin := range orderedBins {
			ch <- bin
		}
	}()
	return ch
}

func (s *SparseStore) orderedBins() []Bin {
	bins := make([]Bin, 0, s.counts.Len())
	s.counts.ForEach(func(index int, count float64) bool {
		bins = append(bins, Bin{index: index, count: count})
		return true
	})
	sort.Slice(bins, func(i, j int) bool { return bins[i].index < bins[j].index })
	return bins
}

func (s *SparseStore) ForEach(f func(index int, count float64) (stop bool)) {
	s.counts.ForEach(func(index int, count float64) bool {
		return !f(index, count)
	})
}

func (s *SparseStore) Copy() Store {
	newStore := NewSparseStore()
	s.counts.ForEach(func(index int, count float64) bool {
		newStore.counts.Put(index, count)
		return true
	})
	return newStore
}

func (s *SparseStore) Clear() {
	s.counts.Clear()
}

func (s *SparseStore) IsEmpty() bool {
	return s.counts.Len() == 0
}

func (s *SparseStore) MaxIndex() (int, error) {
	if s.IsEmpty() {
		return 0, errUndefinedMaxIndex
	}
	maxIndex := minInt
	s.counts.ForEach(func(index int, _ float64) bool {
		if index > maxIndex {
			maxIndex = index
		}
		return true
	})
	return maxIndex, nil
}

func (s *SparseStore) MinIndex() (int, error) {
	if s.IsEmpty() {
		return 0, errUndefinedMinIndex
	}
	minIndex := maxInt
	s.counts.ForEach(func(index int, _ float64) bool {
		if index < minIndex {
			minIndex = index
		}
		return true
	})
	return minIndex, nil
}

func (s *SparseStore) TotalCount() float64 {
	total := 0.0
	s.counts.ForEach(func(_ int, count float64) bool {
		total += count
		return true
	})
	return total
}

func (s *SparseStore) KeyAtRank(rank float64) int {
	orderedBins := s.orderedBins()
	cumulCount := float64(0)
	for _, bin := range orderedBins {
		cumulCount += bin.count
		if cumulCount > rank {
			return bin.index
		}
	}
	if len(orderedBins) > 0 {
		return orderedBins[len(orderedBins)-1].index
	}
	return 0
}

func (s *SparseStore) MergeWith(store Store) {
	store.ForEach(func(index int, count float64) (stop bool) {
		s.AddWithCount(index, count)
		return false
	})
}

func (s *SparseStore) ToProto() *sketchpb.Store {
	binCounts := make(map[int32]float64)
	s.counts.ForEach(func(index int, count float64) bool {
		binCounts[int32(index)] = count
		return true
	})
	return &sketchpb.Store{BinCounts: binCounts}
}

func (s *SparseStore) Reweight(w float64) error {
	if w <= 0 {
		return errors.New("can't reweight by a negative factor")
	}
	if w == 1 {
		return nil
	}

	s.counts.ForEach(func(index int, count float64) bool {
		s.counts.Put(index, count*w)
		return true
	})
	return nil
}

func (s *SparseStore) Encode(b *[]byte, t enc.FlagType) {
	if s.IsEmpty() {
		return
	}
	enc.EncodeFlag(b, enc.NewFlag(t, enc.BinEncodingIndexDeltasAndCounts))
	enc.EncodeUvarint64(b, uint64(s.counts.Len()))
	previousIndex := 0
	bins := make([]Bin, 0, s.counts.Len())
	s.counts.ForEach(func(index int, count float64) bool {
		bins = append(bins, Bin{index: index, count: count})
		return true
	})
	sort.Slice(bins, func(i, j int) bool { return bins[i].index < bins[j].index })
	for _, bin := range bins {
		enc.EncodeVarint64(b, int64(bin.index-previousIndex))
		enc.EncodeVarfloat64(b, bin.count)
		previousIndex = bin.index
	}
}

func (s *SparseStore) DecodeAndMergeWith(b *[]byte, encodingMode enc.SubFlag) error {
	return DecodeAndMergeWith(s, b, encodingMode)
}

var _ Store = (*SparseStore)(nil)
