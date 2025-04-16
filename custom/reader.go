package custom

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"runtime"
	"sc4023/data"
)

type ReaderType int

const (
	FromCsv ReaderType = iota
	FromBinaryInt8
	FromBinaryFloat64
	FromBinaryString
)

// common fields and methods of the custom readers
type Reader interface {
	ReadTo(start int, end int) int
	GetByteOffset() int64
}

type baseReader struct {
	file         *os.File
	byteLimit    int64
	byteOffset   int64
	limitedSlice LimitedSlice
}

// reader to read csv files
type CsvReader struct {
	*baseReader
	reader *csv.Reader
	rowNumber int
}

// reader to read binary data of various types
type BinaryReader[T string | float64 | int8] struct {
	*baseReader
	reader *bufio.Reader
}

func newBaseReader(filePath string, offset int64, limit int64, limitedSlice LimitedSlice) (*baseReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("failed to open file: %s\n", err)
		return nil, err
	}

	_, err = file.Seek(offset, 0)
	if err != nil {
		fmt.Printf("Error seeking to position: %s\n", err)
		return nil, err
	}

	br := &baseReader{
		file:         file,
		byteLimit:    limit,
		byteOffset:   offset,
		limitedSlice: limitedSlice,
	}

	runtime.SetFinalizer(br, func(b *baseReader) { b.file.Close() })

	return br, nil
}

// init new reader depending on type, includes byte offset to read from middle of file and byte limit which when reached
// by the file descriptor stops the reader from reading mroe data
func NewReader(filePath string, offset int64, limit int64, limitedSlice LimitedSlice, readerType ReaderType) Reader {
	br, err := newBaseReader(filePath, offset, limit, limitedSlice)
	if err != nil {
		return nil
	}

	var reader Reader
	switch readerType {
	case FromCsv:
		csvReader := csv.NewReader(br.file)
		reader = &CsvReader{
			reader:     csvReader,
			baseReader: br,
		}
	case FromBinaryInt8:
		binaryReader := bufio.NewReader(br.file)
		reader = &BinaryReader[int8]{
			reader:     binaryReader,
			baseReader: br,
		}
	case FromBinaryFloat64:
		binaryReader := bufio.NewReader(br.file)
		reader = &BinaryReader[float64]{
			reader:     binaryReader,
			baseReader: br,
		}
	case FromBinaryString:
		binaryReader := bufio.NewReader(br.file)
		reader = &BinaryReader[string]{
			reader:     binaryReader,
			baseReader: br,
		}
	default:
		fmt.Println("unknown reader type")
	}

	return reader
}

// loads data from disk and reads to the limited slice, stops when either
// user defined ByteLimit or file EOF is reached, returns numebr of data read
func (r *CsvReader) ReadTo(start int, end int) int {
	readCnt := 0
	for i := start; i <= end; i++ {
		row, err := r.reader.Read()
		r.rowNumber += 1
		r.byteOffset += countCsvBytes(row)
		if err == io.EOF {
			break
		}
		data, err := data.ParseRow(row, r.rowNumber)
		if err != nil {
			r.byteOffset -= countCsvBytes(row)
			i -= 1
			continue
		}
		r.limitedSlice.Set(i, data)
		readCnt += 1
		if r.byteLimit != -1 && r.byteOffset >= r.byteLimit {
			break
		}
	}
	return readCnt
}

// get offset of current file descriptor
func (r *CsvReader) GetByteOffset() int64 {
	return r.byteOffset
}

// loads data from disk (type is based on generic T type paraemter) and reads to the limited slice,
//  stops when either user defined ByteLimit or file EOF is reached, returns numebr of data read
func (r *BinaryReader[T]) ReadTo(start int, end int) int {
	readCnt := 0
	for i := start; i <= end; i++ {
		var val any
		var err error

		switch any(*new(T)).(type) {
		case int8:
			var b byte
			b, err = r.reader.ReadByte()
			val = int8(b)
			r.byteOffset += 1
		case float64:
			var f float64
			err = binary.Read(r.reader, binary.LittleEndian, &f)
			val = f
			r.byteOffset += 8
		case string:
			var strBytes []byte
			strBytes, err = r.reader.ReadBytes('\n')
			if err == nil {
				val = string(strBytes[:len(strBytes)-1])
				r.byteOffset += int64(len(strBytes) + 1)
			}
		default:
			fmt.Printf("ReadTo: unsupported type at index %d\n", i)
			continue
		}

		if err == io.EOF {
			break
		}

		r.limitedSlice.Set(i, val)
		readCnt += 1
		if r.byteLimit != -1 && r.byteOffset >= r.byteLimit {
			break
		}
	}

	return readCnt
}

// get offset of current file descriptor
func (r *BinaryReader[T]) GetByteOffset() int64 {
	return r.byteOffset
}

// count byte width of one csv line
func countCsvBytes(arr []string) int64 {
	var res int64
	for _, str := range arr {
		res += int64(len(str))
	}
	res += 10 // 9 commas and 1 new line
	return res
}
