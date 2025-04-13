package data

import "math"

type Metadata struct {
	Name                string
	Type                any
	DataSizeByte        int64
	RunLengthEncode     bool
	ZoneMapIndexInt8    []ZoneMap[int8]
	ZoneMapIndexFloat64 []ZoneMap[float64]
	BitMapIndex         [][]bool
	OffsetMapIndex      []int64
}

type ZoneMap[T int8 | float64] struct {
	Min T
	Max T
}

func InitColumnStoreMetadata() []*Metadata {
	return []*Metadata{
		{
			Name:             "month",
			Type:             int8(0),
			DataSizeByte:     1,
			RunLengthEncode:  true,
			ZoneMapIndexInt8: []ZoneMap[int8]{},
			OffsetMapIndex:   []int64{},
		},
		{
			Name:            "town",
			Type:            int8(0),
			DataSizeByte:    1,
			RunLengthEncode: true,
			BitMapIndex:     [][]bool{},
			OffsetMapIndex:  []int64{},
		},
		{
			Name:            "flat_type",
			Type:            int8(0),
			DataSizeByte:    1,
			RunLengthEncode: true,
		},
		{
			Name:            "block",
			Type:            "",
			RunLengthEncode: true,
		},
		{
			Name:            "street_name",
			Type:            "",
			RunLengthEncode: true,
		},
		{
			Name:            "storey_range",
			Type:            int8(0),
			DataSizeByte:    1,
			RunLengthEncode: true,
		},
		{
			Name:                "floor_area_sqm",
			Type:                float64(0),
			DataSizeByte:        8,
			RunLengthEncode:     true,
			ZoneMapIndexFloat64: []ZoneMap[float64]{},
			OffsetMapIndex:      []int64{},
		},
		{
			Name:            "flat_model",
			Type:            int8(0),
			DataSizeByte:    1,
			RunLengthEncode: true,
		},
		{
			Name:            "lease_commence_date",
			Type:            int8(0),
			DataSizeByte:    1,
			RunLengthEncode: true,
		},
		{
			Name:                "resale_price",
			Type:                float64(0),
			DataSizeByte:        8,
			RunLengthEncode:     true,
			ZoneMapIndexFloat64: []ZoneMap[float64]{},
			OffsetMapIndex:      []int64{},
		},
	}
}

func (m *Metadata) InitBlockIndexes() {
	if m.ZoneMapIndexInt8 != nil {
		m.ZoneMapIndexInt8 = append(m.ZoneMapIndexInt8, ZoneMap[int8]{Min: math.MaxInt8})
	}
	if m.ZoneMapIndexFloat64 != nil {
		m.ZoneMapIndexFloat64 = append(m.ZoneMapIndexFloat64, ZoneMap[float64]{Min: math.MaxFloat64})
	}
	if m.BitMapIndex != nil {
		bitMap := make([]bool, len(TownToInt)) // only town column will compute bit map
		m.BitMapIndex = append(m.BitMapIndex, bitMap)
	}
	if m.OffsetMapIndex != nil {
		m.OffsetMapIndex = append(m.OffsetMapIndex, 0)
	}
}

func (m *Metadata) UpdateBlockIndexes(val any, blockSize int64) {
	if m.ZoneMapIndexInt8 != nil {
		v := val.(int8)
		currentZoneMap := &m.ZoneMapIndexInt8[len(m.ZoneMapIndexInt8)-1]
		currentZoneMap.Max = max(currentZoneMap.Max, v)
		currentZoneMap.Min = min(currentZoneMap.Min, v)
	}
	if m.ZoneMapIndexFloat64 != nil {
		v := val.(float64)
		currentZoneMap := &m.ZoneMapIndexFloat64[len(m.ZoneMapIndexFloat64)-1]
		currentZoneMap.Max = max(currentZoneMap.Max, v)
		currentZoneMap.Min = min(currentZoneMap.Min, v)
	}
	if m.BitMapIndex != nil {
		m.BitMapIndex[len(m.BitMapIndex)-1][val.(int8)] = true
	}
	if m.OffsetMapIndex != nil {
		m.OffsetMapIndex[len(m.OffsetMapIndex)-1] = blockSize * m.DataSizeByte
	}
}
