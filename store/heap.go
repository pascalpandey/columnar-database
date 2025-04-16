package store

import "sc4023/data"

// struct wrapper for csv row including the sorted chunk index it belongs to
type CsvDataWithIdx struct {
	Data data.CsvData
	Idx  int
}

// heap for merge step in external sort, implements the heap interface in Go stdlib
type DataHeap []CsvDataWithIdx

func (pq DataHeap) Len() int { return len(pq) }

// sort based on month, we can use the dictionary encoding as it is monotonic
func (pq DataHeap) Less(i, j int) bool {
	return data.MonthToInt[pq[i].Data.Month] < data.MonthToInt[pq[j].Data.Month]
}

func (pq DataHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// push data to the heap
func (pq *DataHeap) Push(v any) {
	item := v.(CsvDataWithIdx)
	*pq = append(*pq, item)
}

// pops data with the smallest month value
func (pq *DataHeap) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
