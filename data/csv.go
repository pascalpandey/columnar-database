package data

import (
	"fmt"
	"strconv"
)

type CsvData struct {
	Month         string
	Town          string
	FlatType      string
	Block         string
	StreetName    string
	StoreyRange   string
	FloorArea     float64
	FlatModel     string
	LeaseCommence string
	ResalePrice   float64
}

// deserialize csv row to Go types, returns error on malformed data and ignores the row on subsequent processing
func ParseRow(row []string, rowNumber int) (CsvData, error) {
	if len(row) != 10 {
		fmt.Printf("expected 10 columns per row, got %d in row %d, ignoring row...\n", len(row), rowNumber)
		return CsvData{}, fmt.Errorf("expected 10 columns per row")
	}

	floorArea, err := strconv.ParseFloat(row[6], 64)
	if err != nil || floorArea < 0 {
		fmt.Printf("invalid floor area in row %d, ignoring row...\n", rowNumber)
		return CsvData{}, fmt.Errorf("invalid floor area")
	}

	price, err := strconv.ParseFloat(row[9], 64)
	if err != nil || price < 0 {
		fmt.Printf("invalid price in row %d, ignoring row...\n", rowNumber)
		return CsvData{}, fmt.Errorf("invalid floor price")
	}

	csvData := CsvData{
		Month:         row[0],
		Town:          row[1],
		FlatType:      row[2],
		Block:         row[3],
		StreetName:    row[4],
		StoreyRange:   row[5],
		FloorArea:     floorArea,
		FlatModel:     row[7],
		LeaseCommence: row[8],
		ResalePrice:   price,
	}

	return csvData, nil
}

// format float to the original length in raw data, ensures that byte offsets
// of reader of sorted_chunk.csv is the same as that of raw data
func formatFloat(f float64) string {
	str := fmt.Sprintf("%.2f", f)
	if str[len(str)-2] == '0' {
		return fmt.Sprintf("%.0f", f)
	}
	if str[len(str)-1] == '0' {
		return fmt.Sprintf("%.1f", f)
	}
	return str
}

// to string array for writing to another csv file
func (d CsvData) ToRow() []string {
	return []string{
		d.Month,
		d.Town,
		d.FlatType,
		d.Block,
		d.StreetName,
		d.StoreyRange,
		formatFloat(d.FloorArea),
		d.FlatModel,
		d.LeaseCommence,
		formatFloat(d.ResalePrice),
	}
}

// to individual column data types
func (d CsvData) ToCols() []any {
	return []any{
		MonthToInt[d.Month],
		TownToInt[d.Town],
		FlatTypeToInt[d.FlatType],
		d.Block,
		d.StreetName,
		StoreyRangeToInt[d.StoreyRange],
		d.FloorArea,
		FlatModelToInt[d.FlatModel],
		LeaseCommenceToInt[d.LeaseCommence],
		d.ResalePrice,
	}
}
