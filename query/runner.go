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
	LimitedSlice        custom.LimitedSlice
	ColumnStoreMetadata data.Metadatas
	QueryPlan           []any
	QualifiedBlocks     []int
	TaskQueue           chan int
	wg                  sync.WaitGroup
}

func (q *QueryRunner) InitQueryPlan(month int8, town int8, area float64) {
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

func (q *QueryRunner) RunQuery() []float64 {
	workerSpaceSize := 500
	for i := 0; i < 4; i++ {
		q.wg.Add(1)
		go q.startWorker(i*workerSpaceSize, workerSpaceSize)
	}

	for _, block := range q.QualifiedBlocks {
		q.TaskQueue <- block
	}

	close(q.TaskQueue)

	q.wg.Wait()

	return q.formatResults()
}

func (q *QueryRunner) startWorker(workerIdx int, workerSpace int) {
	defer q.wg.Done()
	for blockIdx := range q.TaskQueue {
		firstFilter := true
		q.LimitedSlice.Reset(workerIdx, workerIdx+workerSpace-1)
		for _, query := range q.QueryPlan {
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

func (q *QueryRunner) handleFilter(isFirstFilter bool, query any, blockIdx, workerIdx, workerSpace int) bool {
	readStart := workerIdx
	readEnd := workerIdx + workerSpace/2 - 1
	writeStart := readEnd + 1
	writeEnd := writeStart + workerSpace/2 - 1
	rwSpace := workerSpace / 2

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

	// check indexes
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
	prevRunLen := 0
	for i := readStart; i < readStart+readCnt; i++ {
		val := q.LimitedSlice.Get(i)
		if length, isRun := utils.CheckRunLength(val); isRun {
			runVal := q.LimitedSlice.Get(i + 1)
			for j := range length {
				if evaluateFilter(query, runVal) && (isFirstFilter || q.LimitedSlice.Get(i+rwSpace+prevRunLen+j) != nil) {
					q.LimitedSlice.Set(i+rwSpace+prevRunLen+j, true)
					hasValidRows = true
				} else {
					q.LimitedSlice.Set(i+rwSpace+prevRunLen+j, nil)
				}
			}
			prevRunLen += length - 2
			i += 1
			continue
		}
		if evaluateFilter(query, val) && (isFirstFilter || q.LimitedSlice.Get(i+prevRunLen+rwSpace) != nil) {
			q.LimitedSlice.Set(i+prevRunLen+rwSpace, true)
			hasValidRows = true
		} else {
			q.LimitedSlice.Set(i+rwSpace+prevRunLen, nil)
		}
	}

	return !hasValidRows
}

func (q *QueryRunner) handleSharedScan(sharedScan SharedScan, blockIdx, workerIdx, workerSpace int) bool {
	readStart := workerIdx
	readEnd := workerIdx + workerSpace/2 - 1
	writeStart := readEnd + 1
	writeEnd := writeStart + workerSpace/2 - 1
	rwSpace := workerSpace / 2

	// if aggregate query is on a specific column load the data and decode with RLE first, in cases like minimum
	// price per area where the query is after an operation, perform the aggregate directly without loading any data
	var readCnt int
	query := sharedScan[0].(*MinQuery)
	if query.Column != nil {
		filePath := fmt.Sprintf("column_store/rle_%s", query.Column.Name)
		offsetByte := query.Column.OffsetMapIndex[blockIdx]
		var limitByte int64
		if blockIdx+1 < len(query.Column.OffsetMapIndex) {
			limitByte = query.Column.OffsetMapIndex[blockIdx+1]
		} else {
			limitByte = -1
		}
		reader := custom.NewReader(filePath, offsetByte, limitByte, q.LimitedSlice, custom.FromBinaryFloat64)
		readCnt = reader.ReadTo(readStart, readEnd)

		prevRunLen := 0
		for i := readStart; i < readStart+readCnt; i++ {
			val := q.LimitedSlice.Get(i)
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
			if q.LimitedSlice.Get(i+prevRunLen+rwSpace) != nil {
				q.LimitedSlice.Set(i+prevRunLen+rwSpace, val)
			}
		}
	} 

	for i := writeStart; i<writeEnd; i++ {
		val := q.LimitedSlice.Get(i)
		if val != nil {
			for _, scan := range sharedScan {
				evaluateAggregate(scan, val)
			}
		}
	}

	return false
}

func (q *QueryRunner) handleOperation(operation *Operation, blockIdx, workerIdx, workerSpace int) bool {
	readStart := workerIdx
	readEnd := workerIdx + workerSpace/2 - 1
	rwSpace := workerSpace / 2

	// assume shared scan is always on the same column
	filePath := fmt.Sprintf("column_store/rle_%s", operation.Column.Name)
	offsetByte := operation.Column.OffsetMapIndex[blockIdx]
	var limitByte int64
	if blockIdx+1 < len(operation.Column.OffsetMapIndex) {
		limitByte = operation.Column.OffsetMapIndex[blockIdx+1]
	} else {
		limitByte = -1
	}
	reader := custom.NewReader(filePath, offsetByte, limitByte, q.LimitedSlice, custom.FromBinaryFloat64)

	// read values and only aggregate values on valid rows
	readCnt := reader.ReadTo(readStart, readEnd)
	prevRunLen := 0
	for i := readStart; i < readStart+readCnt; i++ {
		val := q.LimitedSlice.Get(i)
		if length, isRun := utils.CheckRunLength(val); isRun {
			runVal := q.LimitedSlice.Get(i + 1)
			for j := range length {
				if q.LimitedSlice.Get(i+rwSpace+prevRunLen+j) != nil {
					current := q.LimitedSlice.Get(i + rwSpace + prevRunLen + j)
					switch operation.Op {
					case Add:
						q.LimitedSlice.Set(i+rwSpace+prevRunLen+j, current.(float64)+runVal.(float64))
					case Subtract:
						q.LimitedSlice.Set(i+rwSpace+prevRunLen+j, current.(float64)-runVal.(float64))
					case Multiply:
						q.LimitedSlice.Set(i+rwSpace+prevRunLen+j, current.(float64)*runVal.(float64))
					case Divide:
						q.LimitedSlice.Set(i+rwSpace+prevRunLen+j, current.(float64)/runVal.(float64))
					}
				}
			}
			prevRunLen += length - 2
			i += 1
			continue
		}
		if q.LimitedSlice.Get(i+prevRunLen+rwSpace) != nil {
			current := q.LimitedSlice.Get(i + prevRunLen + rwSpace)
			switch operation.Op {
			case Add:
				q.LimitedSlice.Set(i+prevRunLen+rwSpace, current.(float64)+val.(float64))
			case Subtract:
				q.LimitedSlice.Set(i+prevRunLen+rwSpace, current.(float64)-val.(float64))
			case Multiply:
				q.LimitedSlice.Set(i+prevRunLen+rwSpace, current.(float64)*val.(float64))
			case Divide:
				q.LimitedSlice.Set(i+prevRunLen+rwSpace, current.(float64)/val.(float64))
			}
		}
	}

	return false
}

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