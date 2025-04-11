package utils

var IntToTown = map[int8]string{
	0: "BEDOK",
	1: "BUKIT PANJANG",
	2: "CLEMENTI",
	3: "CHOA CHU KANG",
	4: "HOUGANG",
	5: "JURONG WEST",
	6: "PASIR RIS",
	7: "TAMPINES",
	8: "WOODLANDS",
	9: "YISHUN",
}

var TownToInt = map[string]int8{
	"BEDOK":         0,
	"BUKIT PANJANG": 1,
	"CLEMENTI":      2,
	"CHOA CHU KANG": 3,
	"HOUGANG":       4,
	"JURONG WEST":   5,
	"PASIR RIS":     6,
	"TAMPINES":      7,
	"WOODLANDS":     8,
	"YISHUN":        9,
}
