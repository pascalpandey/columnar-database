package test

import (
	"encoding/csv"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"sc4023/data"
)

// tests that all dtaa are represented in the dictionary and that maping through the dictionary and back leads to the same value
func TestDictionaryMapping(t *testing.T) {
	file, err := os.Open("../ResalePricesSingapore.csv")
	if err != nil {
		t.Fatalf("Failed to open CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Read()

	rowIndex := 1

	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		rowIndex++

		csvData, _ := data.ParseRow(row, rowIndex)

		monthInt, ok := data.MonthToInt[csvData.Month]
		if !ok {
			t.Errorf("Month %s not found in MonthToInt at row %d", csvData.Month, rowIndex)
		} else {
			assert.Equal(t, csvData.Month, data.IntToMonth[monthInt], "Month mismatch at row %d", rowIndex)
		}

		townInt, ok := data.TownToInt[csvData.Town]
		if !ok {
			t.Errorf("Town %s not found in TownToInt at row %d", csvData.Town, rowIndex)
		} else {
			assert.Equal(t, csvData.Town, data.IntToTown[townInt], "Town mismatch at row %d", rowIndex)
		}

		flatTypeInt, ok := data.FlatTypeToInt[csvData.FlatType]
		if !ok {
			t.Errorf("FlatType %s not found in FlatTypeToInt at row %d", csvData.FlatType, rowIndex)
		} else {
			assert.Equal(t, csvData.FlatType, data.IntToFlatType[flatTypeInt], "FlatType mismatch at row %d", rowIndex)
		}

		storeyInt, ok := data.StoreyRangeToInt[csvData.StoreyRange]
		if !ok {
			t.Errorf("StoreyRange %s not found in StoreyRangeToInt at row %d", csvData.StoreyRange, rowIndex)
		} else {
			assert.Equal(t, csvData.StoreyRange, data.IntToStoreyRange[storeyInt], "StoreyRange mismatch at row %d", rowIndex)
		}

		modelInt, ok := data.FlatModelToInt[csvData.FlatModel]
		if !ok {
			t.Errorf("FlatModel %s not found in FlatModelToInt at row %d", csvData.FlatModel, rowIndex)
		} else {
			assert.Equal(t, csvData.FlatModel, data.IntToFlatModel[modelInt], "FlatModel mismatch at row %d", rowIndex)
		}

		leaseInt, ok := data.LeaseCommenceToInt[csvData.LeaseCommence]
		if !ok {
			t.Errorf("LeaseCommence %s not found in LeaseCommenceToInt at row %d", csvData.LeaseCommence, rowIndex)
		} else {
			assert.Equal(t, csvData.LeaseCommence, data.IntToLeaseCommence[leaseInt], "LeaseCommence mismatch at row %d", rowIndex)
		}
	}
}
