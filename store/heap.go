package store

import "sc4023/data"

type CsvDataWithIdx struct {
	Data data.CsvData
	Idx  int
}

type DataHeap []CsvDataWithIdx

func (pq DataHeap) Len() int { return len(pq) }

func (pq DataHeap) Less(i, j int) bool {
	return data.MonthToInt[pq[i].Data.Month] < data.MonthToInt[pq[j].Data.Month]
}

func (pq DataHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *DataHeap) Push(v any) {
	item := v.(CsvDataWithIdx)
	*pq = append(*pq, item)
}

func (pq *DataHeap) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
