package main

import (
	"fmt"
	"sc4023/custom"
	"sc4023/data"
	"sc4023/query"
	"sc4023/store"
	"sc4023/utils"
	"time"
)

func main() {
	utils.CleanDir("./column_store")
	month, town, area, dataPath, matric := utils.ParseFlags()

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

	start := time.Now()
	runner := query.QueryRunner{
		LimitedSlice:        limitedSlice,
		ColumnStoreMetadata: columnStoreMetadata,
		TaskQueue:           make(chan int),
	}
	runner.InitQueryPlan(month, town, area)
	results := runner.RunQuery()

	elapsed := time.Since(start)
	fmt.Printf("Query execution time (excluding column store init): %s\n", elapsed)

	utils.SaveResults(matric, month, town, area, results)
}
