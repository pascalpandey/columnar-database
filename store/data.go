package store

import (
	"fmt"
	"sc4023/utils"
	"strconv"
)

type Data struct {
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

func ParseRow(row []string) Data {
	if len(row) != 10 {
		fmt.Printf("expected 10 columns per row, got %d\n", len(row))
		return Data{}
	}

	floorArea, err := strconv.ParseFloat(row[6], 64)
	if err != nil {
		fmt.Printf("invalid floor area: %s\n", err)
		return Data{}
	}

	price, err := strconv.ParseFloat(row[9], 64)
	if err != nil {
		fmt.Printf("invalid price: %s\n", err)
		return Data{}
	}

	data := Data{
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

	return data
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

func (d Data) toRow() []string {
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

func (d Data) toIndividualCols() []any {
	return []any{
		utils.MonthToInt[d.Month],
		utils.TownToInt[d.Town],
		utils.FlatTypeToInt[d.FlatType],
		d.Block,
		d.StreetName,
		utils.StoreyRangeToInt[d.StoreyRange],
		d.FloorArea,
		utils.FlatModelToInt[d.FlatModel],
		utils.LeaseCommenceToInt[d.LeaseCommence],
		d.ResalePrice,
	}
}
