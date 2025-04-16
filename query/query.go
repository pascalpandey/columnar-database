package query

import (
	"math"
	"sc4023/data"
	"sync"
)

// check whih blocks qualify in a range
type Filter interface {
	GetQualifiedBlocksWithinRange(start, end int) []int
}

// filters rows between InclusiveMin and InclusiveMax
type RangeFilterQuery[T int8 | float64] struct {
	Column       *data.Metadata
	InclusiveMin T
	InclusiveMax T
}

// filters rows based on exact match
type ExactFilterQuery struct {
	Column *data.Metadata
	Match  int8
}

// calculate minimum of a column
type MinQuery struct {
	Column *data.Metadata
	Result float64
	Lock   *sync.Mutex
}

// calculate average of a column
type AvgQuery struct {
	Column  *data.Metadata
	Sum     float64
	NumData int
	Result  float64
	Lock    *sync.Mutex
}

// calculate stdev of a column
type StdevQuery struct {
	Column     *data.Metadata
	Sum        float64
	NumData    int
	SumSquares float64
	Result     float64
	Lock       *sync.Mutex
}

// perform operations on the current loaded data from a column
// with another column, used for minimum price per area query
type OpType int

const (
	Add OpType = iota
	Subtract
	Multiply
	Divide
)

type Operation struct {
	Column *data.Metadata
	Op     OpType
}

// indicates aggregates to be run together
type SharedScan []any

// used by sorted month column to get range of blocks that qualify
func (rfq *RangeFilterQuery[T]) GetQualifiedBlocksRange() (int, int) {
	if !rfq.Column.Sorted {
		return -1, -1
	}
	// only month column is sorted, zone map is int8 there
	start := -1
	end := -1
	for i := range len(rfq.Column.ZoneMapIndexInt8) {
		queryMin := any(rfq.InclusiveMin).(int8)
		queryMax := any(rfq.InclusiveMax).(int8)
		if _, qualified := rfq.Column.ZoneMapIndexInt8[i].Check(queryMin, queryMax); qualified {
			if start == -1 {
				start = i
			}
			end = i
		}
	}
	return start, end
}

// get qualified blocks for range query
func (rfq *RangeFilterQuery[T]) GetQualifiedBlocksWithinRange(start, end int) []int {
	qualBlocks := []int{}
	if rfq.Column.ZoneMapIndexInt8 != nil {
		for i := start; i <= end; i++ {
			zm := rfq.Column.ZoneMapIndexInt8[i]
			queryMin := any(rfq.InclusiveMin).(int8)
			queryMax := any(rfq.InclusiveMax).(int8)
			if _, qualified := zm.Check(queryMin, queryMax); qualified {
				qualBlocks = append(qualBlocks, i)
			}
		}
	}
	if rfq.Column.ZoneMapIndexFloat64 != nil {
		for i := start; i <= end; i++ {
			zm := rfq.Column.ZoneMapIndexFloat64[i]
			queryMin := any(rfq.InclusiveMin).(float64)
			queryMax := any(rfq.InclusiveMax).(float64)
			if _, qualified := zm.Check(queryMin, queryMax); qualified {
				qualBlocks = append(qualBlocks, i)
			}
		}
	}
	return qualBlocks
}

// get qualified blocks for exact query
func (rfq *ExactFilterQuery) GetQualifiedBlocksWithinRange(start, end int) []int {
	qualBlocks := []int{}
	for i := start; i <= end; i++ {
		if _, qualified := rfq.Column.BitMapIndex[i].Check(rfq.Match); qualified {
			qualBlocks = append(qualBlocks, i)
		}
	}
	return qualBlocks
}

// evaluates whether the data is qualified or must be filtered out
func evaluateFilter(query, val any) bool {
	switch query := query.(type) {
	case *RangeFilterQuery[int8]:
		val := val.(int8)
		if val <= query.InclusiveMax && val >= query.InclusiveMin {
			return true
		}
	case *RangeFilterQuery[float64]:
		val := val.(float64)
		if val <= query.InclusiveMax && val >= query.InclusiveMin {
			return true
		}
	case *ExactFilterQuery:
		return val == query.Match
	}
	return false
}

// updates aggregate result based on the data point, need to lock because multiple
// Go routines might be updating the same aggregate query
func evaluateAggregate(query, val any) {
	switch query := query.(type) {
	case *MinQuery:
		query.Lock.Lock()
		query.Result = min(query.Result, val.(float64))
		query.Lock.Unlock()
	case *AvgQuery:
		query.Lock.Lock()
		query.Sum += val.(float64)
		query.NumData += 1
		query.Result = query.Sum / float64(query.NumData)
		query.Lock.Unlock()
	case *StdevQuery:
		query.Lock.Lock()
		query.Sum += val.(float64)
		query.NumData += 1
		query.SumSquares += val.(float64) * val.(float64)
		query.Result = math.Sqrt(query.SumSquares/float64(query.NumData) - math.Pow(query.Sum/float64(query.NumData), 2))
		query.Lock.Unlock()
	}
}

// perform operation between data points from 2 columns
func (op OpType) compute(x, y float64) float64 {
	switch op {
	case Add:
		return x+y
	case Subtract:
		return x-y
	case Multiply:
		return x*y
	case Divide:
		return x/y
	}
	return -1
}
