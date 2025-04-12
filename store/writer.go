package store

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sc4023/limited_slice"
)

type WriterType int

const (
	CSV WriterType = iota
	Binary
)

type Writer interface {
	writeFrom(start int, end int)
}

type baseWriter struct {
	file         *os.File
	limitedSlice *limited_slice.LimitedSlice
}

type CsvWriter struct {
	*baseWriter
	writer *csv.Writer
}

type BinaryWriter struct {
	*baseWriter
	writer *bufio.Writer
}

func newBaseWriter(filePath string, slice *limited_slice.LimitedSlice) (*baseWriter, error) {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	bw := &baseWriter{
		file:         file,
		limitedSlice: slice,
	}

	runtime.SetFinalizer(bw, func(b *baseWriter) { b.file.Close() })

	return bw, nil
}

func newWriter(filePath string, limitedSlice *limited_slice.LimitedSlice, writerType WriterType) Writer {
	bw, err := newBaseWriter(filePath, limitedSlice)
	if err != nil {
		return nil
	}

	var writer Writer
	switch writerType {
	case CSV:
		csvWriter := csv.NewWriter(bw.file)
		writer = &CsvWriter{
			baseWriter: bw,
			writer:     csvWriter,
		}
	case Binary:
		binaryWriter := bufio.NewWriter(bw.file)
		writer = &BinaryWriter{
			baseWriter: bw,
			writer:     binaryWriter,
		}
	default:
		fmt.Println("unknown writer type")
	}

	return writer
}

func (w CsvWriter) writeFrom(start int, end int) {
	for i := start; i <= end; i++ {
		data := w.limitedSlice.Get(i).(Data)
		if err := w.writer.Write(data.toRow()); err != nil {
			fmt.Printf("failed to write data: %s\n", err)
		}
	}

	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		fmt.Printf("failed to flush data: %s\n", err)
	}
}

func (w BinaryWriter) writeFrom(start int, end int) {
	for i := start; i <= end; i++ {
		data := w.limitedSlice.Get(i)
		switch d := data.(type) {
		case int8:
			if err := w.writer.WriteByte(byte(d)); err != nil {
				fmt.Printf("failed to write int8 at %d: %v\n", i, err)
			}
		case float64:
			if err := binary.Write(w.writer, binary.LittleEndian, d); err != nil {
				fmt.Printf("failed to write float64 at %d: %v\n", i, err)
			}
		case string:
			str := d + "\n"
			if _, err := w.writer.WriteString(str); err != nil {
				fmt.Printf("failed to write string at %d: %v\n", i, err)
			}
		default:
			fmt.Printf("unsupported type at index %d: %T\n", i, d)
		}
	}

	if err := w.writer.Flush(); err != nil {
		fmt.Printf("failed to flush writer: %v\n", err)
	}
}
