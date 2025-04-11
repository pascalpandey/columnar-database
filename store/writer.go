package store

import (
	"encoding/csv"
	"path/filepath"
	"fmt"
	"os"
	"runtime"
	"sc4023/limited_slice"
)

type Writer struct {
	file         *os.File
	writer       *csv.Writer
	limitedSlice *limited_slice.LimitedSlice
}

func newWriter(filePath string, slice *limited_slice.LimitedSlice) *Writer {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		fmt.Printf("failed to create directory: %s", err)
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("failed to open file: %s", err)
		return nil
	}

	csvWriter := csv.NewWriter(file)
	writer := &Writer{
		writer:       csvWriter,
		file:         file,
		limitedSlice: slice,
	}

	runtime.SetFinalizer(writer, func(w *Writer) { w.file.Close(); })

	return writer
}

func (w Writer) writeFrom(start int, end int) {
	for i := start; i <= end; i++ {
		data := w.limitedSlice.Get(i).(Data)
		if err := w.writer.Write(data.toRow()); err != nil {
			fmt.Printf("failed to write data: %s", err)
		}
	}

	w.writer.Flush()
    if err := w.writer.Error(); err != nil {
        fmt.Printf("failed to flush data: %s", err)
    }
}
