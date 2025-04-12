package store

import (
	"container/heap"
	"fmt"
	"sc4023/custom"
	"sc4023/data"
	"sc4023/utils"
)

type Store struct {
	LimitedSlice        *custom.LimitedSlice
	DataPath            string
	SortedChunkDataPath string
	SortedDataPath      string
}

func (s Store) InitColumnStore() {
	// sort every chunk of DataPath and write to SortedChunkDataPath, returns byte offset of every chunk
	chunkByteOffset := s.sortChunks()

	// merge sorted chunks to SortedDataPath
	s.mergeSortedChunks(chunkByteOffset)

	// load sorted columns and write each columns to separate files
	s.separateColumns()

	// for all columns compress using run legth encoding
	// for relevant columns compute indexes (zone map, bit map, and/or offset map)
	s.compressAndComputeIndexes()
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
	writer.WriteFrom(numChunks*chunkDataSize, writerIdx-1)
}

func (s Store) separateColumns() {
	cols := 10
	writerIdx := []int{}
	writers := []custom.Writer{}
	colDataSize := s.LimitedSlice.GetLimit() / (cols + 1)
	for i := range cols {
		writers = append(writers, custom.NewWriter(fmt.Sprintf("column_store/%s", data.ColumnMetadata[i].Name), s.LimitedSlice, custom.ToBinary))
		writerIdx = append(writerIdx, i*colDataSize)
	}

	readerIdx := cols * colDataSize
	reader := custom.NewReader(s.SortedDataPath, 0, -1, s.LimitedSlice, custom.FromCsv)
	for {
		dataCnt := reader.ReadTo(readerIdx, s.LimitedSlice.GetLimit()-1)
		if dataCnt == 0 {
			break
		}
		for i := readerIdx; i < readerIdx+dataCnt; i++ {
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
	for col := 0; col < cols; col++ {
		writers[col].WriteFrom(col*colDataSize, writerIdx[col]-1)
		writerIdx[col] = col * colDataSize
	}
}

func (s Store) compressAndComputeIndexes() {
	for _, metadata := range data.ColumnMetadata {
		var reader custom.Reader
		switch metadata.Type.(type) {
		case int8:
			reader = custom.NewReader(fmt.Sprintf("column_store/processed_%s", metadata.Name), 0, -1, s.LimitedSlice, custom.FromBinaryInt8)
		case float64:
			reader = custom.NewReader(fmt.Sprintf("column_store/processed_%s", metadata.Name), 0, -1, s.LimitedSlice, custom.FromBinaryFloat64)
		case string:
			reader = custom.NewReader(fmt.Sprintf("column_store/processed_%s", metadata.Name), 0, -1, s.LimitedSlice, custom.FromBinaryString)
		default:
			fmt.Println("unsupported column type")
			continue
		}
		reader.ReadTo(0, s.LimitedSlice.GetLimit()-1)
		break
	}

}
