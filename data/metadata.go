package data

type Metadata struct {
	Name      string
	Type      any
	Compress  bool
	ZoneMap   []ZoneMap
	BitMap    []bool
	OffsetMap []int64
}

type ZoneMap struct {
	Min int
	Max int
}

var ColumnMetadata = []Metadata{
	{
		Name:      "month",
		Type:      int8(0),
		Compress:  true,
		ZoneMap:   []ZoneMap{},
		OffsetMap: []int64{},
	},
	{
		Name:      "town",
		Type:      int8(0),
		Compress:  true,
		BitMap:    make([]bool, len(TownToInt)),
		OffsetMap: []int64{},
	},
	{
		Name:     "flat_type",
		Type:     int8(0),
		Compress: true,
	},
	{
		Name:     "block",
		Type:     "",
		Compress: true,
	},
	{
		Name:     "street_name",
		Type:     "",
		Compress: true,
	},
	{
		Name:     "storey_range",
		Type:     int8(0),
		Compress: true,
	},
	{
		Name:      "floor_area_sqm",
		Type:      float64(0),
		Compress:  true,
		ZoneMap:   []ZoneMap{},
		OffsetMap: []int64{},
	},
	{
		Name:     "flat_model",
		Type:     int8(0),
		Compress: true,
	},
	{
		Name:     "lease_commence_date",
		Type:     int8(0),
		Compress: true,
	},
	{
		Name:      "resale_price",
		Type:      float64(0),
		Compress:  true,
		ZoneMap:   []ZoneMap{},
		OffsetMap: []int64{},
	},
}
