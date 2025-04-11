package store

import (
	"fmt"
	"strconv"
	"time"
)

type Data struct {
	Month         time.Time
	Town          string
	FlatType      string
	Block         string
	StreetName    string
	StoreyRange   string
	FloorArea     float64
	FlatModel     string
	LeaseCommence int
	ResalePrice   float64
}

func ParseRow(row []string) Data {
	if len(row) != 10 {
		fmt.Printf("expected 10 columns per row, got %d\n", len(row))
		return Data{}
	}

	month, err := time.Parse("2006-01", row[0])
	if err != nil {
		fmt.Printf("invalid date format: %s\n", err)
		return Data{}
	}

	floorArea, err := strconv.ParseFloat(row[6], 64)
	if err != nil {
		fmt.Printf("invalid floor area: %s\n", err)
		return Data{}
	}

	lease, err := strconv.Atoi(row[8])
	if err != nil {
		fmt.Printf("invalid lease year: %s\n", err)
		return Data{}
	}

	price, err := strconv.ParseFloat(row[9], 64)
	if err != nil {
		fmt.Printf("invalid price: %s\n", err)
		return Data{}
	}

	data := Data{
		Month:         month,
		Town:          row[1],
		FlatType:      row[2],
		Block:         row[3],
		StreetName:    row[4],
		StoreyRange:   row[5],
		FloorArea:     floorArea,
		FlatModel:     row[7],
		LeaseCommence: lease,
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
		d.Month.Format("2006-01"),
		d.Town,
		d.FlatType,
		d.Block,
		d.StreetName,
		d.StoreyRange,
		formatFloat(d.FloorArea),
		d.FlatModel,
		fmt.Sprintf("%d", d.LeaseCommence),
		formatFloat(d.ResalePrice),
	}
}
