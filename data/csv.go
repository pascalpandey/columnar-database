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

func ParseRow(row []string) CsvData {
	if len(row) != 10 {
		fmt.Printf("expected 10 columns per row, got %d\n", len(row))
		return CsvData{}
	}

	floorArea, err := strconv.ParseFloat(row[6], 64)
	if err != nil {
		fmt.Printf("invalid floor area: %s\n", err)
		return CsvData{}
	}

	price, err := strconv.ParseFloat(row[9], 64)
	if err != nil {
		fmt.Printf("invalid price: %s\n", err)
		return CsvData{}
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

	return csvData
}

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
