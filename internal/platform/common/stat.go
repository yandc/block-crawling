package common

import (
	"math"
	"time"
)

type Stater interface {
	Put(time.Duration, bool)
	Peek() *StatSnapshot
}
type StatSnapshot struct {
	Mean    time.Duration
	Max     time.Duration
	Min     time.Duration
	Success uint64
	Total   uint64
}

type stat struct {
	totalElapsed time.Duration
	totalNum     uint64
	success      uint64
	maxElapsed   time.Duration
	minElapsed   time.Duration
}

func NewStat() Stater {
	return &stat{
		minElapsed: math.MaxInt64,
	}
}

func (s *stat) Put(elapsed time.Duration, success bool) {
	s.totalNum++
	s.totalElapsed += elapsed
	if success {
		s.success++
	}
	if elapsed < s.minElapsed {
		s.minElapsed = elapsed
	}
	if elapsed > s.maxElapsed {
		s.maxElapsed = elapsed
	}
}

func (s *stat) Peek() *StatSnapshot {
	var mean time.Duration
	if s.totalNum > 0 {
		mean = s.totalElapsed / time.Duration(s.totalNum)
	}
	return &StatSnapshot{
		Mean:    mean,
		Max:     s.maxElapsed,
		Min:     s.minElapsed,
		Success: s.success,
		Total:   s.totalNum,
	}
}
