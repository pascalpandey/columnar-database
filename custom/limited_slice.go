package custom

import "sort"

type LimitedSlice struct {
	data  []any
	limit int
}

func InitLimitedSlice(limit int) *LimitedSlice {
	return &LimitedSlice{
		data:  make([]any, limit),
		limit: limit,
	}
}

func (l *LimitedSlice) Get(index int) any {
	return l.data[index]
}

func (l *LimitedSlice) Set(index int, item any) {
	l.data[index] = item
}

func (l *LimitedSlice) Sort(start int, end int, fn func(i, j int) bool) {
	sort.Slice(l.data[start:end+1], fn)
}

func (l *LimitedSlice) GetLimit() int {
	return l.limit
}
