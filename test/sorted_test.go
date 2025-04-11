package test

import (
	"encoding/csv"
	"flag"
	"os"
	"sc4023/store"
	"testing"
	"time"
)

var filePath string
var sortedFilePath string

func init() {
	flag.StringVar(&filePath, "data", "", "Path to CSV data file")
	flag.StringVar(&sortedFilePath, "sorted", "", "Path to CSV data file")
}

func TestIsFileSorted(t *testing.T) {
	file, err := os.Open(sortedFilePath)
	if err != nil {
		t.Fatalf("Error opening file: %s", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	var prevDate time.Time
	act := 0

	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		data := store.ParseRow(row)
		currentDate := data.Month

		if !currentDate.After(prevDate) && !currentDate.Equal(prevDate) && prevDate != (time.Time{}) {
			t.Errorf("Error: Date is not sorted at row %d. Previous: %s, Current: %s\n",
				act+1, prevDate.Format("2006-01"), currentDate.Format("2006-01"))
		}

		prevDate = currentDate
		act++
	}
}

func TestFilesHaveSameDataUnordered(t *testing.T) {
	file1, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Error opening first file: %s", err)
	}
	defer file1.Close()

	file2, err := os.Open(sortedFilePath)
	if err != nil {
		t.Fatalf("Error opening second file: %s", err)
	}
	defer file2.Close()

	rows1 := readAllRows(file1, true)
	rows2 := readAllRows(file2, false)

	// if len(rows1) != len(rows2) {
	// 	t.Fatalf("Row count mismatch: %d != %d", len(rows1), len(rows2))
	// }

	counts1 := make(map[string]int)
	counts2 := make(map[string]int)

	for _, r := range rows1 {
		counts1[serializeRow(r)]++
	}
	for _, r := range rows2 {
		counts2[serializeRow(r)]++
	}

	for k, v := range counts1 {
		if counts2[k] != v {
			t.Errorf("Mismatch in row: %q => expected %d, got %d", k, v, counts2[k])
		}
	}
}

func readAllRows(f *os.File, skipHeader bool) [][]string {
	reader := csv.NewReader(f)
	if skipHeader {
		reader.Read()
	}
	var rows [][]string
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		rows = append(rows, row)
	}
	return rows
}

func serializeRow(row []string) string {
	out := ""
	for i, v := range row {
		if i > 0 {
			out += ","
		}
		out += v
	}
	return out
}
