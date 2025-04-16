package test

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sc4023/data"
	"sc4023/utils"
	"testing"
)

// test that the run length encoded columns can be decoded and is equivalent to the original column data
func TestRLE(t *testing.T) {
	metadatas := data.InitColumnStoreMetadata()
	for _, metadata := range metadatas {
		rawPath := fmt.Sprintf("../column_store/raw_%s", metadata.Name)
		rawFile, err := os.Open(rawPath)
		if err != nil {
			t.Fatalf("failed to open file: %s\n", err)
		}
		rawReader := bufio.NewReader(rawFile)

		rlePath := fmt.Sprintf("../column_store/rle_%s", metadata.Name)
		rleFile, err := os.Open(rlePath)
		if err != nil {
			t.Fatalf("failed to open file: %s\n", err)
		}
		rleReader := bufio.NewReader(rleFile)

		idx := 0
		lgth := 0
		var valRle any
		for {
			valRaw, errRaw := read(rawReader, metadata.Type)
			if errRaw == io.EOF {
				break
			}

			if lgth > 0 { // active run
				if valRle != valRaw {
					t.Fatalf("mismatch of raw value %v and rle value %v in row %v in col %v", valRaw, valRle, idx, metadata.Name)
				}
				lgth -= 1
				idx += 1
				continue
			}

			valRle, _ = read(rleReader, metadata.Type)
			if lg, isRun := utils.CheckRunLength(valRle); isRun {
				lgth = lg
				valRle, _ = read(rleReader, metadata.Type)
				if valRle != valRaw {
					t.Fatalf("mismatch of raw value %v and rle value %v in row %v in col %v", valRaw, valRle, idx, metadata.Name)
				}
				lgth -= 1
			} else {
				if valRle != valRaw {
					t.Fatalf("mismatch of raw value %v and rle value %v in row %v in col %v", valRaw, valRle, idx, metadata.Name)
				}
			}
			idx += 1
		}
	}
}

// helper to read data and serialize to Go type
func read(reader *bufio.Reader, colType any) (any, error) {
	var v any
	var err error
	switch colType.(type) {
	case int8:
		var b byte
		b, err = reader.ReadByte()
		v = int8(b)
	case float64:
		var f float64
		err = binary.Read(reader, binary.LittleEndian, &f)
		v = f
	case string:
		var strBytes []byte
		strBytes, err = reader.ReadBytes('\n')
		if err == nil {
			v = string(strBytes[:len(strBytes)-1])
		}
	}
	return v, err
}