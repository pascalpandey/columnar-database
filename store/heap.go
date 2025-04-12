package store

import "sc4023/utils"

type DataWithIdx struct {
	Data Data
	Idx  int
}

type DataHeap []DataWithIdx

func (pq DataHeap) Len() int { return len(pq) }

func (pq DataHeap) Less(i, j int) bool {
	return utils.MonthToInt[pq[i].Data.Month] < utils.MonthToInt[pq[j].Data.Month]
}

func (pq DataHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *DataHeap) Push(v any) {
	item := v.(DataWithIdx)
	*pq = append(*pq, item)
}

func (pq *DataHeap) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
