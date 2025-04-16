package store

import (
	"container/heap"
	"fmt"
	"sc4023/custom"
	"sc4023/data"
	"sc4023/utils"
	"strconv"
)

type Store struct {
	LimitedSlice        custom.LimitedSlice // limited buffer, all operations must happen here without external allocations
	DataPath            string              // path of raw csv
	SortedChunkDataPath string              // path of csv with sorted chunks (on month)
	SortedDataPath      string              // path of final sorted csv (on month)
	ColumnStoreMetadata data.Metadatas      // metadata of each column store column
}

func (s Store) InitColumnStore() {
	// sort every chunk of DataPath and write to SortedChunkDataPath, returns byte offset of every chunk
	chunkByteOffset := s.sortChunks()

	// merge sorted chunks to SortedDataPath
	s.mergeSortedChunks(chunkByteOffset)

	// load sorted columns and write each columns to separate files
	s.separateColumns()

	// for all columns compress using run length encoding
	// for relevant columns compute indexes (zone map, bit map, and/or offset map)
	s.processColumns()
}

// sort every 2000 rows and write to SortedChunkDataPath, this is the first step for external sort
func (s Store) sortChunks() []int64 {
	headerByte := utils.CountHeaderByte(s.DataPath) // skip raw csv data header
	reader := custom.NewReader(s.DataPath, int64(headerByte), -1, s.LimitedSlice, custom.FromCsv)
	writer := custom.NewWriter(s.SortedChunkDataPath, s.LimitedSlice, custom.ToCsv)

	chunkByteOffset := []int64{} // used to indicate byte offsets of sorted chunk for the merge step
	for {
		chunkByteOffset = append(chunkByteOffset, reader.GetByteOffset()-int64(headerByte))

		// laod data and sort every 2000 data chunk
		readCnt := reader.ReadTo(0, s.LimitedSlice.GetLimit()-1)
		s.LimitedSlice.Sort(0, readCnt-1, func(i, j int) bool {
			return data.MonthToInt[s.LimitedSlice.Get(i).(data.CsvData).Month] < data.MonthToInt[s.LimitedSlice.Get(j).(data.CsvData).Month]
		})

		// write back to SortedChunkDataPath
		writer.WriteFrom(0, readCnt-1)

		if readCnt == 0 {
			break
		}
	}
	return chunkByteOffset
}

// merge all 2000 wide chunks into a single file, this is the second step for external sort
func (s Store) mergeSortedChunks(chunkByteOffset []int64) {
	// initialize readers based on the previous sorted chunk offsets
	readerIdx := []int{}
	readers := []custom.Reader{}
	numChunks := len(chunkByteOffset) - 1
	readerDataLeft := make([]int, numChunks)
	chunkDataSize := s.LimitedSlice.GetLimit() / (numChunks + 1)
	for i := range numChunks {
		readerIdx = append(readerIdx, i*chunkDataSize)
		if i == numChunks-1 {
			readers = append(readers, custom.NewReader(s.SortedChunkDataPath, chunkByteOffset[i], -1, s.LimitedSlice, custom.FromCsv))
		} else {
			readers = append(readers, custom.NewReader(s.SortedChunkDataPath, chunkByteOffset[i], chunkByteOffset[i+1], s.LimitedSlice, custom.FromCsv))
		}
	}

	// writer to final sorted file 
	writerIdx := numChunks * chunkDataSize
	writer := custom.NewWriter(s.SortedDataPath, s.LimitedSlice, custom.ToCsv)

	// initialize heap with the first data of every sorted chunk
	h := DataHeap{}
	for i, r := range readers {
		readCnt := r.ReadTo(readerIdx[i], readerIdx[i]+chunkDataSize-1)
		readerDataLeft[i] = readCnt - 1
		h = append(h, CsvDataWithIdx{Data: s.LimitedSlice.Get(readerIdx[i]).(data.CsvData), Idx: i})
	}

	// until the heap is empty perform the following:
	// 1. when chunk buffer is empty load data from file starting from ChunkByteOffset (stored inside reader)
	// 2. pop csv data with smallest month value, increment chunk pointer, and load the next data on the chunk
	// 3. move the just popped data to the writer buffer, when sorted writer buffer is full write to SortedDataPath
	heap.Init(&h)
	for len(h) > 0 {
		// get data with smallest month value
		item := heap.Pop(&h).(CsvDataWithIdx)
		i := item.Idx
		csvData := item.Data

		// increment the reader idnex of the chunk where the smallest data belongs to
		readerIdx[i] += 1
		if readerIdx[i] == (i+1)*chunkDataSize {
			readerIdx[i] = i * chunkDataSize
			readCnt := readers[i].ReadTo(readerIdx[i], readerIdx[i]+chunkDataSize-1)
			readerDataLeft[i] = readCnt
		}
		if readerDataLeft[i] > 0 {
			heap.Push(&h, CsvDataWithIdx{Data: s.LimitedSlice.Get(readerIdx[i]).(data.CsvData), Idx: i})
			readerDataLeft[i] -= 1
		}

		// copy the data to the writer index and write to file when buffer is full
		s.LimitedSlice.Set(writerIdx, csvData)
		writerIdx += 1
		if writerIdx == s.LimitedSlice.GetLimit() {
			writer.WriteFrom(numChunks*chunkDataSize, s.LimitedSlice.GetLimit()-1)
			writerIdx = numChunks * chunkDataSize
		}
	}

	// write the rest of the data, because we only write when writer buffer is full
	// the previous iteration might not have written yet
	writer.WriteFrom(numChunks*chunkDataSize, writerIdx-1)
}

// separate each row from the sorted csv into individual columns to `column_store/raw_<column_name>`
func (s Store) separateColumns() {
	// intialize bianry writers for each column
	cols := 10
	writerIdx := []int{}
	writers := []custom.Writer{}
	colDataSize := s.LimitedSlice.GetLimit() / (cols + 1)
	for i := range cols {
		writers = append(writers, custom.NewWriter(fmt.Sprintf("column_store/raw_%s", s.ColumnStoreMetadata[i].Name), s.LimitedSlice, custom.ToBinary))
		writerIdx = append(writerIdx, i*colDataSize)
	}

	// initialize reader to read from SortedDataPath
	readerIdx := cols * colDataSize
	reader := custom.NewReader(s.SortedDataPath, 0, -1, s.LimitedSlice, custom.FromCsv)
	for {
		// load data from sorted csv file 2000 rows at a time
		readCnt := reader.ReadTo(readerIdx, s.LimitedSlice.GetLimit()-1)
		if readCnt == 0 {
			break
		}
		
		// for each csv row, split the data and write to the respective columns
		for i := readerIdx; i < readerIdx+readCnt; i++ {
			dataCols := s.LimitedSlice.Get(i).(data.CsvData).ToCols() // performs dictionary encoding as well
			for col := 0; col < cols; col++ {
				s.LimitedSlice.Set(writerIdx[col], dataCols[col])
				writerIdx[col] += 1
				if writerIdx[col] == (col+1)*colDataSize {
					writers[col].WriteFrom(col*colDataSize, (col+1)*colDataSize-1)
					writerIdx[col] = col * colDataSize
				}
			}
		}
	}

	// write the rest of the data, because we only write when writer buffer is full
	// the previous iteration might not have written yet
	for col := 0; col < cols; col++ {
		writers[col].WriteFrom(col*colDataSize, writerIdx[col]-1)
	}
}

// process each column again, perform RLE and compute indexes, this writes to `column_store/rle_<column_name>`
func (s Store) processColumns() {
	// process each column at a time
	for _, metadata := range s.ColumnStoreMetadata {
		// initialize the appropriate reader based on column type
		var reader custom.Reader
		switch metadata.Type.(type) {
		case int8:
			reader = custom.NewReader(fmt.Sprintf("column_store/raw_%s", metadata.Name), 0, -1, s.LimitedSlice, custom.FromBinaryInt8)
		case float64:
			reader = custom.NewReader(fmt.Sprintf("column_store/raw_%s", metadata.Name), 0, -1, s.LimitedSlice, custom.FromBinaryFloat64)
		case string:
			reader = custom.NewReader(fmt.Sprintf("column_store/raw_%s", metadata.Name), 0, -1, s.LimitedSlice, custom.FromBinaryString)
		default:
			fmt.Println("unsupported column type")
			continue
		}

		// writer to the specified column file
		writer := custom.NewWriter(fmt.Sprintf("column_store/rle_%s", metadata.Name), s.LimitedSlice, custom.ToBinary)

		// perform RLE, we have readerIdx which reads data and writerIdx to indicate which part of the buffer to write back to 
		// file writerIdx and readerIdx both start at 0, writerIdx will reuse the space that readerIdx has already read to ensure
		// I/O is minimized as much as possible
		// at the same time perform index computation, we asusme indexes are much smaller than the data, in this case indexes
		// are 1/250th of the raw data (each block is 250 and we index per block) so we store this directly in memory
		blockSize := 250
		prevWriterIdx := 0 // accumulate writerIdx for offset map calculation
		for {
			readCnt := reader.ReadTo(0, s.LimitedSlice.GetLimit()-1)
			if readCnt == 0 {
				break
			}
			runIdx := -1
			writerIdx := 0
			readerIdx := 0
			for readerIdx < readCnt {
				// this is a new block, so initialize fresh indexes
				if readerIdx%blockSize == 0 {
					metadata.InitBlockIndexes(int64(prevWriterIdx + writerIdx))
				}
				current := s.LimitedSlice.Get(readerIdx)
				metadata.UpdateBlockIndexes(current) // for each value, update the indexes in the current block

				// if column is to be encoded perform RLE encding, we use negatie values do distinguish between
				// run length and actual data
				if metadata.RunLengthEncode {
					if runIdx == -1 { // no active run, check if there's repeated data, if there is start the run
						if readerIdx > 0 && current == s.LimitedSlice.Get(readerIdx-1) {
							s.LimitedSlice.Set(writerIdx-1, -2)
							runIdx = writerIdx - 1
						}
					} else { // active run ongoing, increment if the read value is the same as the run value
						runLength := s.LimitedSlice.Get(runIdx).(int)
						_, isInt8 := metadata.Type.(int8)
						// int8 can ony store up to 128, so check if we need to end the run also end the run
						// if we reach the end of our 250 blocks
						if s.LimitedSlice.Get(runIdx+1) != current || (isInt8 && runLength == -128) || readerIdx%blockSize == 0 {
							s.endEncodingRun(&runIdx, metadata.Type)
						} else {
							s.LimitedSlice.Set(runIdx, runLength-1)
							readerIdx += 1
							continue
						}
					}
				}

				// writerIdx might be behind readerIdx so we have to write data to the writerIdx
				s.LimitedSlice.Set(writerIdx, current)
				writerIdx += 1
				readerIdx += 1
			}

			// flush data based on writerIdx and end ongoing runs
			prevWriterIdx += writerIdx
			s.endEncodingRun(&runIdx, metadata.Type)
			writer.WriteFrom(0, writerIdx-1)
		}
	}
}

// end encoding run by setting runIdx to -1 and updating type of run length into the column type
func (s Store) endEncodingRun(runIdx *int, colType any) {
	if *runIdx == -1 {
		return
	}
	switch colType.(type) {
	case int8:
		s.LimitedSlice.Set(*runIdx, int8(s.LimitedSlice.Get(*runIdx).(int)))
	case float64:
		s.LimitedSlice.Set(*runIdx, float64(s.LimitedSlice.Get(*runIdx).(int)))
	case string:
		s.LimitedSlice.Set(*runIdx, strconv.Itoa(s.LimitedSlice.Get(*runIdx).(int)))
	}
	*runIdx = -1
}
