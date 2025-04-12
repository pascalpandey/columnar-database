package test

import (
	"encoding/csv"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"sc4023/store"
	"sc4023/utils"
)

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

		data := store.ParseRow(row)

		monthInt, ok := utils.MonthToInt[data.Month]
		if !ok {
			t.Errorf("Month %s not found in MonthToInt at row %d", data.Month, rowIndex)
		} else {
			assert.Equal(t, data.Month, utils.IntToMonth[monthInt], "Month mismatch at row %d", rowIndex)
		}

		townInt, ok := utils.TownToInt[data.Town]
		if !ok {
			t.Errorf("Town %s not found in TownToInt at row %d", data.Town, rowIndex)
		} else {
			assert.Equal(t, data.Town, utils.IntToTown[townInt], "Town mismatch at row %d", rowIndex)
		}

		flatTypeInt, ok := utils.FlatTypeToInt[data.FlatType]
		if !ok {
			t.Errorf("FlatType %s not found in FlatTypeToInt at row %d", data.FlatType, rowIndex)
		} else {
			assert.Equal(t, data.FlatType, utils.IntToFlatType[flatTypeInt], "FlatType mismatch at row %d", rowIndex)
		}

		storeyInt, ok := utils.StoreyRangeToInt[data.StoreyRange]
		if !ok {
			t.Errorf("StoreyRange %s not found in StoreyRangeToInt at row %d", data.StoreyRange, rowIndex)
		} else {
			assert.Equal(t, data.StoreyRange, utils.IntToStoreyRange[storeyInt], "StoreyRange mismatch at row %d", rowIndex)
		}

		modelInt, ok := utils.FlatModelToInt[data.FlatModel]
		if !ok {
			t.Errorf("FlatModel %s not found in FlatModelToInt at row %d", data.FlatModel, rowIndex)
		} else {
			assert.Equal(t, data.FlatModel, utils.IntToFlatModel[modelInt], "FlatModel mismatch at row %d", rowIndex)
		}

		leaseInt, ok := utils.LeaseCommenceToInt[data.LeaseCommence]
		if !ok {
			t.Errorf("LeaseCommence %s not found in LeaseCommenceToInt at row %d", data.LeaseCommence, rowIndex)
		} else {
			assert.Equal(t, data.LeaseCommence, utils.IntToLeaseCommence[leaseInt], "LeaseCommence mismatch at row %d", rowIndex)
		}
	}
}
