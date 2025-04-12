package test

import (
	"encoding/csv"
	"os"
	"sc4023/data"
	"testing"
)

func TestFileSortedByMonth(t *testing.T) {
	file, err := os.Open("../column_store/sorted.csv")
	if err != nil {
		t.Fatalf("Error opening file: %s", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	var prevDate string
	act := 0

	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		csvData := data.ParseRow(row)
		currentDate := csvData.Month

		if prevDate != "" && data.MonthToInt[currentDate] < data.MonthToInt[prevDate] {
			t.Errorf("Error: Date is not sorted at row %d. Previous: %s, Current: %s\n",
				act+1, prevDate, currentDate)
		}

		prevDate = currentDate
		act++
	}
}

func TestSortedAndOriginalHaveSameData(t *testing.T) {
	file1, err := os.Open("../ResalePricesSingapore.csv")
	if err != nil {
		t.Fatalf("Error opening first file: %s", err)
	}
	defer file1.Close()

	file2, err := os.Open("../column_store/sorted.csv")
	if err != nil {
		t.Fatalf("Error opening second file: %s", err)
	}
	defer file2.Close()

	rows1 := readAllRows(file1, true)
	rows2 := readAllRows(file2, false)

	if len(rows1) != len(rows2) {
		t.Fatalf("Row count mismatch: %d != %d", len(rows1), len(rows2))
	}

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
