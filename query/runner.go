package query

import (
	"fmt"
	"math"
	"sc4023/custom"
	"sc4023/data"
	"sc4023/utils"
	"sort"
	"sync"
)

type QueryRunner struct {
	LimitedSlice        custom.LimitedSlice // limited slice where queries are run
	ColumnStoreMetadata data.Metadatas      // metadata of each column
	QueryPlan           []any               // query plan to be executed by each worker
	QualifiedBlocks     []int               // qualified blocks from initial filtering on the sorted month column
	TaskQueue           chan int            // channel for distributing tasks between workers
	wg                  sync.WaitGroup      // wait group to wait until all workers finish execution
}

// initialize the query plan, this will be run by each worker which processes each qualified block absed on this plan
func (q *QueryRunner) InitQueryPlan(month int8, town int8, area float64) {
	// intialize plan and filters
	plan := []any{}
	filters := []Filter{}
	filterMonth := RangeFilterQuery[int8]{
		Column:       q.ColumnStoreMetadata.GetColMetadata("month"),
		InclusiveMin: month,
		InclusiveMax: month + 1,
	}
	filterTown := ExactFilterQuery{
		Column: q.ColumnStoreMetadata.GetColMetadata("town"),
		Match:  town,
	}
	filterArea := RangeFilterQuery[float64]{
		Column:       q.ColumnStoreMetadata.GetColMetadata("floor_area_sqm"),
		InclusiveMin: area,
		InclusiveMax: math.MaxFloat64,
	}

	// get range of qualified blocks from the sorted month column, then sort filters
	// based on the least number of qualified blocks
	start, end := filterMonth.GetQualifiedBlocksRange()
	filters = append(filters, &filterMonth, &filterTown, &filterArea)
	sort.Slice(filters, func(i, j int) bool {
		return len(filters[i].GetQualifiedBlocksWithinRange(start, end)) <
			len(filters[j].GetQualifiedBlocksWithinRange(start, end))
	})

	// get the initial qualifying blocks and append filters to the plan
	q.QualifiedBlocks = filters[0].GetQualifiedBlocksWithinRange(start, end)
	for _, filter := range filters {
		plan = append(plan, filter)
	}

	// min, average, and stdev price can be queried together with shared scan
	priceCol := q.ColumnStoreMetadata.GetColMetadata("resale_price")
	plan = append(plan, SharedScan{
		&MinQuery{Column: priceCol, Lock: &sync.Mutex{}, Result: math.MaxFloat64},
		&AvgQuery{Column: priceCol, Lock: &sync.Mutex{}},
		&StdevQuery{Column: priceCol, Lock: &sync.Mutex{}},
	})

	// for min price per area, need to perform division operation first, then do a MinQuery with column nil
	areaCol := q.ColumnStoreMetadata.GetColMetadata("floor_area_sqm")
	plan = append(plan, &Operation{Column: areaCol, Op: Divide})
	plan = append(plan, SharedScan{&MinQuery{Lock: &sync.Mutex{}, Result: math.MaxFloat64}}) // represent as single shared scan to reduce code

	q.QueryPlan = plan
}

// entrypoint of running the query, divides limited slice into 4 workspaces of 500 elements each
// each worker will run on this workspace and processes each block independently, each worker uses
// the first 250 elements as read space to load data and the next 250 elements as write space
// to store filter or load results
func (q *QueryRunner) RunQuery() []float64 {
	// start workers
	workerSpaceSize := 500
	for i := 0; i < 4; i++ {
		q.wg.Add(1)
		go q.startWorker(i*workerSpaceSize, workerSpaceSize)
	}

	// distribute tasks into the channel
	for _, block := range q.QualifiedBlocks {
		q.TaskQueue <- block
	}

	// close the channel and wait until all workers are done
	close(q.TaskQueue)
	q.wg.Wait()

	return q.formatResults()
}

// starts a worker, it consumes qualified blocks from the channel and processes it
func (q *QueryRunner) startWorker(workerIdx int, workerSpace int) {
	defer q.wg.Done()
	for blockIdx := range q.TaskQueue {
		firstFilter := true
		q.LimitedSlice.Reset(workerIdx, workerIdx+workerSpace-1) // reset worker space in case of leftover queries from previous blocks
		for _, query := range q.QueryPlan {
			// run appropriate process depending on the query type
			var done bool
			switch query := query.(type) {
			case *RangeFilterQuery[int8], *RangeFilterQuery[float64], *ExactFilterQuery:
				done = q.handleFilter(firstFilter, query, blockIdx, workerIdx, workerSpace)
			case SharedScan:
				done = q.handleSharedScan(query, blockIdx, workerIdx, workerSpace)
			case *Operation:
				done = q.handleOperation(query, blockIdx, workerIdx, workerSpace)
			}
			if done {
				break
			}
			firstFilter = false
		}
	}
}

// perform filtering, first laod data into the read space and write booleans into the write space, treating the write space
// as a bit map for inidcating whether index of data in the data block qualifies or not, data bocks are 250 wide so its 
// guaranteed to fit in the read space
func (q *QueryRunner) handleFilter(isFirstFilter bool, query any, blockIdx, workerIdx, workerSpace int) bool {
	// split the 500 element workerSpace into read and write space of 250 elements each
	readStart := workerIdx
	readEnd := workerIdx + workerSpace/2 - 1
	writeStart := readEnd + 1
	writeEnd := writeStart + workerSpace/2 - 1
	rwSpace := workerSpace / 2

	// init reader appropriately based on query column type and store results of index checks to handle afterwards
	var skippable, qualified bool
	var reader custom.Reader
	var limitByte int64
	switch query := query.(type) {
	case *RangeFilterQuery[int8]:
		filePath := fmt.Sprintf("column_store/rle_%s", query.Column.Name)
		offsetByte := query.Column.OffsetMapIndex[blockIdx]
		if blockIdx+1 < len(query.Column.OffsetMapIndex) {
			limitByte = query.Column.OffsetMapIndex[blockIdx+1]
		} else {
			limitByte = -1
		}
		skippable, qualified = query.Column.ZoneMapIndexInt8[blockIdx].Check(query.InclusiveMin, query.InclusiveMax)
		reader = custom.NewReader(filePath, offsetByte, limitByte, q.LimitedSlice, custom.FromBinaryInt8)
	case *RangeFilterQuery[float64]:
		filePath := fmt.Sprintf("column_store/rle_%s", query.Column.Name)
		offsetByte := query.Column.OffsetMapIndex[blockIdx]
		if blockIdx+1 < len(query.Column.OffsetMapIndex) {
			limitByte = query.Column.OffsetMapIndex[blockIdx+1]
		} else {
			limitByte = -1
		}
		skippable, qualified = query.Column.ZoneMapIndexFloat64[blockIdx].Check(query.InclusiveMin, query.InclusiveMax)
		reader = custom.NewReader(filePath, offsetByte, limitByte, q.LimitedSlice, custom.FromBinaryFloat64)
	case *ExactFilterQuery:
		filePath := fmt.Sprintf("column_store/rle_%s", query.Column.Name)
		offsetByte := query.Column.OffsetMapIndex[blockIdx]
		if blockIdx+1 < len(query.Column.OffsetMapIndex) {
			limitByte = query.Column.OffsetMapIndex[blockIdx+1]
		} else {
			limitByte = -1
		}
		skippable, qualified = query.Column.BitMapIndex[blockIdx].Check(query.Match)
		reader = custom.NewReader(filePath, offsetByte, limitByte, q.LimitedSlice, custom.FromBinaryInt8)
	}

	// check indexes using the previously stored results
	if skippable {
		if qualified {
			// if qualified and not the first filter continue with previous valid rows
			if !isFirstFilter {
				return false
			}
			// else fill the writer buffer with all rows as valid
			for i := writeStart; i < writeEnd; i++ {
				q.LimitedSlice.Set(i, true)
			}
			return false
		} else {
			// if not qualified immediately return done
			return true
		}
	}

	// index unable to determine valid rows, so we load data and filter manually
	hasValidRows := false
	readCnt := reader.ReadTo(readStart, readEnd)
	prevRunLen := 0 // helps to write to the write index at the write space
	for i := readStart; i < readStart+readCnt; i++ {
		val := q.LimitedSlice.Get(i)
		// check if its an RLE run, and handle appropriately
		if length, isRun := utils.CheckRunLength(val); isRun {
			runVal := q.LimitedSlice.Get(i + 1)
			for j := range length {
				// if qualified and previous row also qualifies or is the first filter set the row bit map to true
				if evaluateFilter(query, runVal) && (isFirstFilter || q.LimitedSlice.Get(i+rwSpace+prevRunLen+j) != nil) {
					q.LimitedSlice.Set(i+rwSpace+prevRunLen+j, true)
					hasValidRows = true
				} else { // unqualified row, so reset the index
					q.LimitedSlice.Set(i+rwSpace+prevRunLen+j, nil)
				}
			}
			prevRunLen += length - 2
			i += 1
			continue
		}

		// not an RLE run so continue as normal
		if evaluateFilter(query, val) && (isFirstFilter || q.LimitedSlice.Get(i+prevRunLen+rwSpace) != nil) {
			q.LimitedSlice.Set(i+prevRunLen+rwSpace, true)
			hasValidRows = true
		} else {
			q.LimitedSlice.Set(i+rwSpace+prevRunLen, nil)
		}
	}

	return !hasValidRows
}

// handle shared scans, which are aggregate queries, there are 2 types aggregates on a column and on existing data
// for aggregates on a column, first laod the data from disk, otherwise directly read the existing data and compute results
func (q *QueryRunner) handleSharedScan(sharedScan SharedScan, blockIdx, workerIdx, workerSpace int) bool {
	readStart := workerIdx
	readEnd := workerIdx + workerSpace/2 - 1
	writeStart := readEnd + 1
	writeEnd := writeStart + workerSpace/2 - 1
	rwSpace := workerSpace / 2

	// if aggregate query is on a specific column load the data and decode with RLE first, in cases like minimum
	// price per area where the query is after an operation, perform the aggregate directly without loading any data
	query := sharedScan[0].(*MinQuery)
	if query.Column != nil {
		// intiialize reader
		filePath := fmt.Sprintf("column_store/rle_%s", query.Column.Name)
		offsetByte := query.Column.OffsetMapIndex[blockIdx]
		var limitByte int64
		if blockIdx+1 < len(query.Column.OffsetMapIndex) {
			limitByte = query.Column.OffsetMapIndex[blockIdx+1]
		} else {
			limitByte = -1
		}
		reader := custom.NewReader(filePath, offsetByte, limitByte, q.LimitedSlice, custom.FromBinaryFloat64)
		readCnt := reader.ReadTo(readStart, readEnd)

		// perform run length decoding and write valid data to the write space, only write to indexes
		// which are valid, i.e. data is non null, this is to make sure that we only laod data rows which
		// are valid after filtering, which maye be done in previous steps
		prevRunLen := 0
		for i := readStart; i < readStart+readCnt; i++ {
			val := q.LimitedSlice.Get(i)
			// check if its an RLE run, and handle appropriately
			if length, isRun := utils.CheckRunLength(val); isRun {
				runVal := q.LimitedSlice.Get(i + 1)
				for j := range length {
					if q.LimitedSlice.Get(i+rwSpace+prevRunLen+j) != nil {
						q.LimitedSlice.Set(i+rwSpace+prevRunLen+j, runVal)
					}
				}
				prevRunLen += length - 2
				i += 1
				continue
			}

			// not RLE run so continue normally
			if q.LimitedSlice.Get(i+prevRunLen+rwSpace) != nil {
				q.LimitedSlice.Set(i+prevRunLen+rwSpace, val)
			}
		}
	}

	// perform shared scan on the valid loaded data
	for i := writeStart; i < writeEnd; i++ {
		val := q.LimitedSlice.Get(i)
		if val != nil {
			for _, scan := range sharedScan {
				evaluateAggregate(scan, val)
			}
		}
	}

	return false
}

// perform designated operation on the current worker space, first load data form the other column then perform
// operations on the currently stored data in the write space
func (q *QueryRunner) handleOperation(operation *Operation, blockIdx, workerIdx, workerSpace int) bool {
	readStart := workerIdx
	readEnd := workerIdx + workerSpace/2 - 1
	rwSpace := workerSpace / 2

	// initialize reader
	filePath := fmt.Sprintf("column_store/rle_%s", operation.Column.Name)
	offsetByte := operation.Column.OffsetMapIndex[blockIdx]
	var limitByte int64
	if blockIdx+1 < len(operation.Column.OffsetMapIndex) {
		limitByte = operation.Column.OffsetMapIndex[blockIdx+1]
	} else {
		limitByte = -1
	}
	reader := custom.NewReader(filePath, offsetByte, limitByte, q.LimitedSlice, custom.FromBinaryFloat64)

	// read values and perform operation only if the row is valid
	readCnt := reader.ReadTo(readStart, readEnd)
	prevRunLen := 0
	for i := readStart; i < readStart+readCnt; i++ {
		val := q.LimitedSlice.Get(i)
		if length, isRun := utils.CheckRunLength(val); isRun { // is RLE run so stay in this idx and handle it
			runVal := q.LimitedSlice.Get(i + 1)
			for j := range length {
				writerIdx := i + rwSpace + prevRunLen + j
				if q.LimitedSlice.Get(writerIdx) != nil {
					current := q.LimitedSlice.Get(writerIdx)
					q.LimitedSlice.Set(writerIdx, operation.Op.compute(current.(float64), runVal.(float64)))
				}
			}
			prevRunLen += length - 2
			i += 1
			continue
		}

		// not RLE so continue as normal
		writerIdx := i + prevRunLen + rwSpace
		if q.LimitedSlice.Get(writerIdx) != nil {
			current := q.LimitedSlice.Get(writerIdx)
			q.LimitedSlice.Set(writerIdx, operation.Op.compute(current.(float64), val.(float64)))
		}
	}

	return false
}

// checks the query plan for SharedScan type and extracts the query results
func (q *QueryRunner) formatResults() []float64 {
	res := []float64{}
	for _, query := range q.QueryPlan {
		switch query := query.(type) {
		case SharedScan:
			for _, scan := range query {
				switch scan := scan.(type) {
				case *MinQuery:
					res = append(res, scan.Result)
				case *AvgQuery:
					res = append(res, scan.Result)
				case *StdevQuery:
					res = append(res, scan.Result)
				}
			}
		}
	}
	return res
}
