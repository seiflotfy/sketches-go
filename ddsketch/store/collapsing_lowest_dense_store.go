// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package store

import (
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

// CollapsingLowestDenseStore is a dynamically growing contiguous (non-sparse) store.
// The lower bins get combined so that the total number of bins do not exceed maxNumBins.
type CollapsingLowestDenseStore struct {
	DenseStore
	maxNumBins int32
}

func NewCollapsingLowestDenseStore(maxNumBins int32) *CollapsingLowestDenseStore {
	// Bins are not allocated until values are added.
	// When the first value is added, a small number of bins are allocated. The number of bins will
	// grow as needed up to maxNumBins.
	return &CollapsingLowestDenseStore{maxNumBins: maxNumBins}
}

func (s *CollapsingLowestDenseStore) Add(index int32) {
	s.addWithCount(index, float64(1))
}

func (s *CollapsingLowestDenseStore) AddBins(bin Bin) {
	index := bin.Index()
	count := bin.Count()
	if count == 0 {
		return
	}
	s.addWithCount(index, count)
}

func (s *CollapsingLowestDenseStore) addWithCount(index int32, count float64) {
	if s.count == 0 {
		s.bins = make([]float64, min(growthBuffer, s.maxNumBins))
		s.maxIndex = index
		s.minIndex = index - int32(len(s.bins)) + 1
	}
	if index < s.minIndex {
		s.growLeft(index)
	} else if index > s.maxIndex {
		s.growRight(index)
	}
	var idx int32
	if index < s.minIndex {
		idx = 0
	} else {
		idx = index - s.minIndex
	}
	s.bins[idx] += count
	s.count += count
}

func (s *CollapsingLowestDenseStore) growLeft(index int32) {
	if s.minIndex < index || int32(len(s.bins)) >= s.maxNumBins {
		return
	}

	var minIndex int32
	if s.maxIndex >= index+s.maxNumBins {
		minIndex = s.maxIndex - s.maxNumBins + 1
	} else {
		// Expand bins by up to an extra growthBuffer bins than strictly required.
		minIndex = max(index-growthBuffer, s.maxIndex-s.maxNumBins+1)
	}
	tmpBins := make([]float64, s.maxIndex-minIndex+1)
	copy(tmpBins[s.minIndex-minIndex:], s.bins)
	s.bins = tmpBins
	s.minIndex = minIndex
}

func (s *CollapsingLowestDenseStore) growRight(index int32) {
	if s.maxIndex > index {
		return
	}
	if index >= s.maxIndex+s.maxNumBins {
		s.bins = make([]float64, s.maxNumBins)
		s.maxIndex = index
		s.minIndex = index - s.maxNumBins + 1
		s.bins[0] = s.count
	} else if index >= s.minIndex+s.maxNumBins {
		minIndex := index - s.maxNumBins + 1
		var n float64
		for i := s.minIndex; i <= min(minIndex-1, s.maxIndex); i++ {
			n += s.bins[i-s.minIndex]
		}
		if int32(len(s.bins)) < s.maxNumBins {
			tmpBins := make([]float64, s.maxNumBins)
			copy(tmpBins, s.bins[minIndex-s.minIndex:])
			s.bins = tmpBins
		} else {
			copy(s.bins, s.bins[minIndex-s.minIndex:])
			for i := s.maxIndex - minIndex + 1; i < s.maxNumBins; i++ {
				s.bins[i] = 0.0
			}
		}
		s.maxIndex = index
		s.minIndex = minIndex
		s.bins[0] += n
	} else {
		tmpBins := make([]float64, index-s.minIndex+1)
		copy(tmpBins, s.bins)
		s.bins = tmpBins
		s.maxIndex = index
	}
}

func (s *CollapsingLowestDenseStore) MergeWith(other Store) {
	if other.TotalCount() == 0 {
		return
	}
	o, ok := other.(*CollapsingLowestDenseStore)
	if !ok {
		for bin := range other.Bins() {
			s.AddBin(bin)
		}
		return
	}
	if s.count == 0 {
		s.copy(o)
		return
	}
	s.growRight(o.maxIndex)
	s.growLeft(o.minIndex)
	for i := max(s.minIndex, o.minIndex); i <= min(s.maxIndex, o.maxIndex); i++ {
		s.bins[i-s.minIndex] += o.bins[i-o.minIndex]
	}
	var n float64
	for i := o.minIndex; i <= min(s.minIndex-1, o.maxIndex); i++ {
		n += o.bins[i-o.minIndex]
	}
	s.bins[0] += n
	s.count += o.count
}

func (s *CollapsingLowestDenseStore) copy(o *CollapsingLowestDenseStore) {
	s.bins = make([]float64, len(o.bins))
	copy(s.bins, o.bins)
	s.minIndex = o.minIndex
	s.maxIndex = o.maxIndex
	s.count = o.count
	s.maxNumBins = o.maxNumBins
}

func (s *CollapsingLowestDenseStore) makeCopy() *CollapsingLowestDenseStore {
	bins := make([]float64, len(s.bins))
	copy(bins, s.bins)
	return &CollapsingLowestDenseStore{
		DenseStore: DenseStore{
			bins:     bins,
			count:    s.count,
			minIndex: s.minIndex,
			maxIndex: s.maxIndex,
		},
		maxNumBins: s.maxNumBins,
	}
}

func (s *CollapsingLowestDenseStore) FromProto(pb *sketchpb.Store) {
	// Reset the store.
	s.count = 0
	s.bins = nil
	s.minIndex = 0
	s.maxIndex = 0
	for idx, count := range pb.BinCounts {
		s.addWithCount(idx, count)
	}
	for idx, count := range pb.ContiguousBinCounts {
		s.addWithCount(int32(idx)+pb.ContiguousBinIndexOffset, count)
	}
}

func max(x, y int32) int32 {
	if x > y {
		return x
	}
	return y
}

func min(x, y int32) int32 {
	if x < y {
		return x
	}
	return y
}
