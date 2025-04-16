package custom

import "sort"

// limited slice struct where all data loading and processing happens
// lowercase field names mean private fields, to ensure limited slice is unmodifiable
type LimitedSlice struct {
	data  []any
	limit int
}

// init limited slice
func InitLimitedSlice(limit int) LimitedSlice {
	return LimitedSlice{
		data:  make([]any, limit),
		limit: limit,
	}
}

// get data at index
func (l *LimitedSlice) Get(index int) any {
	return l.data[index]
}

// set data at index
func (l *LimitedSlice) Set(index int, item any) {
	l.data[index] = item
}

// sort data from start to end using fn
func (l *LimitedSlice) Sort(start int, end int, fn func(i, j int) bool) {
	sort.Slice(l.data[start:end+1], fn)
}

// get size of limited slice
func (l *LimitedSlice) GetLimit() int {
	return l.limit
}

// reset data in start to end to nil
func (l *LimitedSlice) Reset(start, end int) {
	for i := start; i <= end; i++ {
		l.Set(i, nil)
	}
}
