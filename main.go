package main

import (
	"sc4023/custom"
	"sc4023/data"
	"sc4023/store"
	"sc4023/utils"
)

func main() {
	utils.CleanDir("./column_store")
	_, _, dataPath := utils.ParseFlags()

	// Simulate big data environment by only allowing loading of 2000 data points at any time
	limitedSlice := custom.InitLimitedSlice(2000)

	sortedChunkDataPath := "./column_store/sorted_chunk.csv"
	sortedDataPath := "./column_store/sorted.csv"
	columnStoreMetadata := data.InitColumnStoreMetadata()
	store := store.Store{
		LimitedSlice:        limitedSlice,
		DataPath:            dataPath,
		SortedChunkDataPath: sortedChunkDataPath,
		SortedDataPath:      sortedDataPath,
		ColumnStoreMetadata: columnStoreMetadata,
	}
	store.InitColumnStore()
}
