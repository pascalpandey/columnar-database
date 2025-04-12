package store

import (
	"container/heap"
	"fmt"
	"sc4023/limited_slice"
	"sc4023/utils"
)

type Store struct {
	LimitedSlice        *limited_slice.LimitedSlice
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
	// for relevant columns (month, town, area, and price) compute indexes (zone map, bloom filter, and offset map)
	s.compressAndComputeIndexes()
}

func (s Store) sortChunks() []int64 {
	headerByte := utils.CountHeaderByte(s.DataPath)
	reader := newReader(s.DataPath, int64(headerByte), -1, s.LimitedSlice)
	writer := newWriter(s.SortedChunkDataPath, s.LimitedSlice, CSV)

	chunkByteOffset := []int64{}
	for {
		chunkByteOffset = append(chunkByteOffset, reader.ByteOffset-int64(headerByte))
		readCnt := reader.readTo(0, s.LimitedSlice.GetLimit()-1)

		s.LimitedSlice.Sort(0, readCnt-1, func(i, j int) bool {
			return utils.MonthToInt[s.LimitedSlice.Get(i).(Data).Month] < utils.MonthToInt[s.LimitedSlice.Get(j).(Data).Month]
		})
		writer.writeFrom(0, readCnt-1)

		if readCnt == 0 {
			break
		}
	}
	return chunkByteOffset
}

func (s Store) mergeSortedChunks(chunkByteOffset []int64) {
	readerIdx := []int{}
	readers := []*Reader{}
	numChunks := len(chunkByteOffset) - 1
	readerDataLeft := make([]int, numChunks)
	chunkDataSize := s.LimitedSlice.GetLimit() / (numChunks + 1)
	for i := range numChunks {
		readerIdx = append(readerIdx, i*chunkDataSize)
		if i == numChunks-1 {
			readers = append(readers, newReader(s.SortedChunkDataPath, chunkByteOffset[i], -1, s.LimitedSlice))
		} else {
			readers = append(readers, newReader(s.SortedChunkDataPath, chunkByteOffset[i], chunkByteOffset[i+1], s.LimitedSlice))
		}
	}

	writerIdx := numChunks * chunkDataSize
	writer := newWriter(s.SortedDataPath, s.LimitedSlice, CSV)

	h := DataHeap{}
	for i, r := range readers {
		readCnt := r.readTo(readerIdx[i], readerIdx[i]+chunkDataSize-1)
		readerDataLeft[i] = readCnt - 1
		h = append(h, DataWithIdx{Data: s.LimitedSlice.Get(readerIdx[i]).(Data), Idx: i})
	}

	heap.Init(&h)
	for len(h) > 0 {
		item := heap.Pop(&h).(DataWithIdx)
		i := item.Idx
		data := item.Data

		readerIdx[i] += 1
		if readerIdx[i] == (i+1)*chunkDataSize {
			readerIdx[i] = i * chunkDataSize
			readCnt := readers[i].readTo(readerIdx[i], readerIdx[i]+chunkDataSize-1)
			readerDataLeft[i] = readCnt
		}
		if readerDataLeft[i] > 0 {
			heap.Push(&h, DataWithIdx{Data: s.LimitedSlice.Get(readerIdx[i]).(Data), Idx: i})
			readerDataLeft[i] -= 1
		}

		s.LimitedSlice.Set(writerIdx, data)
		writerIdx += 1
		if writerIdx == s.LimitedSlice.GetLimit() {
			writer.writeFrom(numChunks*chunkDataSize, s.LimitedSlice.GetLimit()-1)
			writerIdx = numChunks * chunkDataSize
		}
	}
	writer.writeFrom(numChunks*chunkDataSize, writerIdx-1)
}

func (s Store) separateColumns() {
	cols := 10
	columnNames := []string{"month", "town", "flat_type", "block", "street_name",
		"storey_range", "floor_area_sqm", "flat_model", "lease_commence_date", "resale_price"}
	writerIdx := []int{}
	writers := []Writer{}
	colDataSize := s.LimitedSlice.GetLimit() / (cols + 1)
	for i := range cols {
		writers = append(writers, newWriter(fmt.Sprintf("column_store/%s", columnNames[i]), s.LimitedSlice, Binary))
		writerIdx = append(writerIdx, i*colDataSize)
	}

	readerIdx := cols * colDataSize
	reader := newReader(s.SortedDataPath, 0, -1, s.LimitedSlice)
	for {
		dataCnt := reader.readTo(readerIdx, s.LimitedSlice.GetLimit()-1)
		if dataCnt == 0 {
			break
		}
		for i := readerIdx; i < readerIdx+dataCnt; i++ {
			data := s.LimitedSlice.Get(i).(Data).toIndividualCols()
			for col := 0; col < cols; col++ {
				s.LimitedSlice.Set(writerIdx[col], data[col])
				writerIdx[col] += 1
				if writerIdx[col] == (col+1)*colDataSize {
					writers[col].writeFrom(col*colDataSize, (col+1)*colDataSize-1)
					writerIdx[col] = col * colDataSize
				}
			}
		}
	}
	for col := 0; col < cols; col++ {
		writers[col].writeFrom(col*colDataSize, writerIdx[col]-1)
		writerIdx[col] = col * colDataSize
	}
}

func (s Store) compressAndComputeIndexes() {
	
}
