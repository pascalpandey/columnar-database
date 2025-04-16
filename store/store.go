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
	LimitedSlice        custom.LimitedSlice
	DataPath            string
	SortedChunkDataPath string
	SortedDataPath      string
	ColumnStoreMetadata data.Metadatas
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

func (s Store) sortChunks() []int64 {
	headerByte := utils.CountHeaderByte(s.DataPath)
	reader := custom.NewReader(s.DataPath, int64(headerByte), -1, s.LimitedSlice, custom.FromCsv)
	writer := custom.NewWriter(s.SortedChunkDataPath, s.LimitedSlice, custom.ToCsv)

	chunkByteOffset := []int64{}
	for {
		chunkByteOffset = append(chunkByteOffset, reader.GetByteOffset()-int64(headerByte))
		readCnt := reader.ReadTo(0, s.LimitedSlice.GetLimit()-1)

		s.LimitedSlice.Sort(0, readCnt-1, func(i, j int) bool {
			return data.MonthToInt[s.LimitedSlice.Get(i).(data.CsvData).Month] < data.MonthToInt[s.LimitedSlice.Get(j).(data.CsvData).Month]
		})
		writer.WriteFrom(0, readCnt-1)

		if readCnt == 0 {
			break
		}
	}
	return chunkByteOffset
}

func (s Store) mergeSortedChunks(chunkByteOffset []int64) {
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

	writerIdx := numChunks * chunkDataSize
	writer := custom.NewWriter(s.SortedDataPath, s.LimitedSlice, custom.ToCsv)

	h := DataHeap{}
	for i, r := range readers {
		readCnt := r.ReadTo(readerIdx[i], readerIdx[i]+chunkDataSize-1)
		readerDataLeft[i] = readCnt - 1
		h = append(h, CsvDataWithIdx{Data: s.LimitedSlice.Get(readerIdx[i]).(data.CsvData), Idx: i})
	}

	heap.Init(&h)
	for len(h) > 0 {
		item := heap.Pop(&h).(CsvDataWithIdx)
		i := item.Idx
		csvData := item.Data

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

func (s Store) separateColumns() {
	cols := 10
	writerIdx := []int{}
	writers := []custom.Writer{}
	colDataSize := s.LimitedSlice.GetLimit() / (cols + 1)
	for i := range cols {
		writers = append(writers, custom.NewWriter(fmt.Sprintf("column_store/raw_%s", s.ColumnStoreMetadata[i].Name), s.LimitedSlice, custom.ToBinary))
		writerIdx = append(writerIdx, i*colDataSize)
	}

	readerIdx := cols * colDataSize
	reader := custom.NewReader(s.SortedDataPath, 0, -1, s.LimitedSlice, custom.FromCsv)
	for {
		readCnt := reader.ReadTo(readerIdx, s.LimitedSlice.GetLimit()-1)
		if readCnt == 0 {
			break
		}
		for i := readerIdx; i < readerIdx+readCnt; i++ {
			dataCols := s.LimitedSlice.Get(i).(data.CsvData).ToCols()
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

func (s Store) processColumns() {
	for _, metadata := range s.ColumnStoreMetadata {
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

		writer := custom.NewWriter(fmt.Sprintf("column_store/rle_%s", metadata.Name), s.LimitedSlice, custom.ToBinary)

		blockSize := 250
		prevWriterIdx := 0
		for {
			readCnt := reader.ReadTo(0, s.LimitedSlice.GetLimit()-1)
			if readCnt == 0 {
				break
			}
			runIdx := -1
			writerIdx := 0
			readerIdx := 0
			for readerIdx < readCnt {
				if readerIdx%blockSize == 0 {
					metadata.InitBlockIndexes(int64(prevWriterIdx+writerIdx))
				}
				current := s.LimitedSlice.Get(readerIdx)
				metadata.UpdateBlockIndexes(current)
				if metadata.RunLengthEncode {
					if runIdx == -1 { // no active run
						if readerIdx > 0 && current == s.LimitedSlice.Get(readerIdx-1) {
							s.LimitedSlice.Set(writerIdx-1, -2)
							runIdx = writerIdx-1
						}
					} else { // active run ongoing
						runLength := s.LimitedSlice.Get(runIdx).(int)
						_, isInt8 := metadata.Type.(int8)
						if s.LimitedSlice.Get(runIdx+1) != current || (isInt8 && runLength == -128) || readerIdx%blockSize == 0 {
							s.endEncodingRun(&runIdx, metadata.Type)
						} else {
							s.LimitedSlice.Set(runIdx, runLength-1)
							readerIdx += 1
							continue
						}
					}
				}
				s.LimitedSlice.Set(writerIdx, current)
				writerIdx += 1
				readerIdx += 1
			}
			prevWriterIdx += writerIdx
			s.endEncodingRun(&runIdx, metadata.Type)
			writer.WriteFrom(0, writerIdx-1)
		}
	}
}

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
