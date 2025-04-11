package store

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"runtime"
	"sc4023/limited_slice"
)

type Reader struct {
	file         *os.File
	reader       *csv.Reader
	byteLimit    int64
	ByteOffset   int64
	limitedSlice *limited_slice.LimitedSlice
}

func newReader(filePath string, offset int64, limit int64, slice *limited_slice.LimitedSlice) *Reader {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("failed to open file: %s\n", err)
		return nil
	}

	_, err = file.Seek(offset, 0)
	if err != nil {
		fmt.Println("Error seeking to position:", err)
	}

	csvReader := csv.NewReader(file)
	reader := &Reader{
		reader:       csvReader,
		file:         file,
		byteLimit:    limit,
		ByteOffset:   offset,
		limitedSlice: slice,
	}

	runtime.SetFinalizer(reader, func(r *Reader) { r.file.Close() })

	return reader
}

func (r *Reader) readTo(start int, end int) int {
	readCnt := 0
	for i := start; i <= end; i++ {
		row, err := r.reader.Read()
		r.ByteOffset += countBytes(row)
		if err == io.EOF {
			return readCnt
		}
		r.limitedSlice.Set(i, ParseRow(row))
		readCnt += 1
		if r.byteLimit != -1 && r.ByteOffset >= r.byteLimit {
			return readCnt
		}
	}
	return readCnt
}

func countBytes(arr []string) int64 {
	var res int64
	for _, str := range arr  {
		res += int64(len(str))
	}
	res += 10 // 9 commas and 1 new line
	return res
}