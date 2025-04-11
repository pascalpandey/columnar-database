package main

import (
	"sc4023/limited_slice"
	"sc4023/store"
	"sc4023/utils"
)

func main() {
	utils.CleanDir("./column_store")
	_, _, _, dataPath := utils.ParseFlags()

	// Simulate big data environment by only allowing loading of 2000 data points at any time
	limitedSlice := limited_slice.InitLimitedSlice(2000)

	sortedChunkDataPath := "./column_store/sorted_chunk.csv"
	sortedDataPath := "./column_store/sorted.csv"
	store := store.Store{
		LimitedSlice:        limitedSlice,
		DataPath:            dataPath,
		SortedChunkDataPath: sortedChunkDataPath,
		SortedDataPath:      sortedDataPath,
	}
	store.InitColumnStore()
}
