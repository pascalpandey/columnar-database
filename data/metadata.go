package data

import "math"

type Metadatas []*Metadata

type Metadata struct {
	Name                string             // name of column
	Type                any                // data type
	DataSizeByte        int64              // size of data type in bytes
	Sorted              bool               // whether or not col is sorted
	RunLengthEncode     bool               // whether or not col is run length encoded
	ZoneMapIndexInt8    []ZoneMap[int8]    // zone map for int8 cols
	ZoneMapIndexFloat64 []ZoneMap[float64] // zone map for float64 cols
	BitMapIndex         []Bitmap           // bit map for exact queries
	OffsetMapIndex      []int64            // byte offsets of each data block
}

// init column store metadata to be used by main Store and QueryRunner structs
func InitColumnStoreMetadata() Metadatas {
	return Metadatas{
		{
			Name:             "month",
			Type:             int8(0),
			DataSizeByte:     1,
			Sorted:           true,
			RunLengthEncode:  true,
			ZoneMapIndexInt8: []ZoneMap[int8]{},
			OffsetMapIndex:   []int64{},
		},
		{
			Name:            "town",
			Type:            int8(0),
			DataSizeByte:    1,
			RunLengthEncode: true,
			BitMapIndex:     []Bitmap{},
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

// create indexes for new data block
func (m *Metadata) InitBlockIndexes(blockSize int64) {
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
		m.OffsetMapIndex = append(m.OffsetMapIndex, blockSize*m.DataSizeByte)
	}
}

// update latest block indexes
func (m *Metadata) UpdateBlockIndexes(val any) {
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
}

// get metadata based on column name
func (ms Metadatas) GetColMetadata(name string) *Metadata {
	for _, metadata := range ms {
		if metadata.Name == name {
			return metadata
		}
	}
	return nil
}
