package custom

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sc4023/data"
)

type WriterType int

const (
	ToCsv WriterType = iota
	ToBinary
)

// common methods and fields of CsvWriter and BinaryWriter
type Writer interface {
	WriteFrom(start int, end int)
}

type baseWriter struct {
	file         *os.File
	limitedSlice LimitedSlice
}

// writer for csv files
type CsvWriter struct {
	*baseWriter
	writer *csv.Writer
}

// writer for all binary files, automatically detects data in limited slice
// and writes the appropriate type to the file
type BinaryWriter struct {
	*baseWriter
	writer *bufio.Writer
}

func newBaseWriter(filePath string, limitedSlice LimitedSlice) (*baseWriter, error) {
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
		limitedSlice: limitedSlice,
	}

	runtime.SetFinalizer(bw, func(b *baseWriter) { b.file.Close() })

	return bw, nil
}

// init writer depending on file type
func NewWriter(filePath string, limitedSlice LimitedSlice, writerType WriterType) Writer {
	bw, err := newBaseWriter(filePath, limitedSlice)
	if err != nil {
		return nil
	}

	var writer Writer
	switch writerType {
	case ToCsv:
		csvWriter := csv.NewWriter(bw.file)
		writer = &CsvWriter{
			baseWriter: bw,
			writer:     csvWriter,
		}
	case ToBinary:
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

// write to csv file rows from start to end
func (w CsvWriter) WriteFrom(start int, end int) {
	for i := start; i <= end; i++ {
		csvData := w.limitedSlice.Get(i).(data.CsvData)
		if err := w.writer.Write(csvData.ToRow()); err != nil {
			fmt.Printf("failed to write data: %s\n", err)
		}
	}

	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		fmt.Printf("failed to flush data: %s\n", err)
	}
}

// write to binary file data from start to end, automaticlaly detects
// data type and writes appropriately to the binary file
func (w BinaryWriter) WriteFrom(start int, end int) {
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
			fmt.Printf("WriteFrom: unsupported type at index %d: %T, %v\n", i, d, data)
		}
	}

	if err := w.writer.Flush(); err != nil {
		fmt.Printf("failed to flush writer: %v\n", err)
	}
}
