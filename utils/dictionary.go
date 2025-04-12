package utils

import (
	"strconv"
	"time"
)

// month column
var MonthToInt, IntToMonth = func() (map[string]int8, map[int8]string) {
	startTime, _ := time.Parse("2006-01", "2014-01")
	endTime, _ := time.Parse("2006-01", "2024-01")

	monthToInt := map[string]int8{}
	intToMonth := map[int8]string{}
	idx := 0

	for t := startTime; !t.After(endTime); t = t.AddDate(0, 1, 0) {
		key := t.Format("2006-01")
		monthToInt[key] = int8(idx)
		intToMonth[int8(idx)] = key
		idx++
	}

	return monthToInt, intToMonth
}()

// town column
var IntToTown = map[int8]string{
	0:  "BEDOK",
	1:  "BUKIT PANJANG",
	2:  "CLEMENTI",
	3:  "CHOA CHU KANG",
	4:  "HOUGANG",
	5:  "JURONG WEST",
	6:  "PASIR RIS",
	7:  "TAMPINES",
	8:  "WOODLANDS",
	9:  "YISHUN",
	10: "ANG MO KIO",
	11: "BISHAN",
	12: "BUKIT BATOK",
	13: "BUKIT MERAH",
	14: "BUKIT TIMAH",
	15: "CENTRAL AREA",
	16: "GEYLANG",
	17: "JURONG EAST",
	18: "KALLANG/WHAMPOA",
	19: "MARINE PARADE",
	20: "PUNGGOL",
	21: "QUEENSTOWN",
	22: "SEMBAWANG",
	23: "SENGKANG",
	24: "SERANGOON",
	25: "TOA PAYOH",
}

var TownToInt = map[string]int8{
	"BEDOK":           0,
	"BUKIT PANJANG":   1,
	"CLEMENTI":        2,
	"CHOA CHU KANG":   3,
	"HOUGANG":         4,
	"JURONG WEST":     5,
	"PASIR RIS":       6,
	"TAMPINES":        7,
	"WOODLANDS":       8,
	"YISHUN":          9,
	"ANG MO KIO":      10,
	"BISHAN":          11,
	"BUKIT BATOK":     12,
	"BUKIT MERAH":     13,
	"BUKIT TIMAH":     14,
	"CENTRAL AREA":    15,
	"GEYLANG":         16,
	"JURONG EAST":     17,
	"KALLANG/WHAMPOA": 18,
	"MARINE PARADE":   19,
	"PUNGGOL":         20,
	"QUEENSTOWN":      21,
	"SEMBAWANG":       22,
	"SENGKANG":        23,
	"SERANGOON":       24,
	"TOA PAYOH":       25,
}

// flat_type column
var FlatTypeToInt = map[string]int8{
	"1 ROOM":           0,
	"2 ROOM":           1,
	"3 ROOM":           2,
	"4 ROOM":           3,
	"5 ROOM":           4,
	"MULTI-GENERATION": 5,
	"EXECUTIVE":        6,
}

var IntToFlatType = map[int8]string{
	0: "1 ROOM",
	1: "2 ROOM",
	2: "3 ROOM",
	3: "4 ROOM",
	4: "5 ROOM",
	5: "MULTI-GENERATION",
	6: "EXECUTIVE",
}

// storey_range column
var StoreyRangeToInt = map[string]int8{
	"01 TO 03": 0,
	"04 TO 06": 1,
	"07 TO 09": 2,
	"10 TO 12": 3,
	"13 TO 15": 4,
	"16 TO 18": 5,
	"19 TO 21": 6,
	"22 TO 24": 7,
	"25 TO 27": 8,
	"28 TO 30": 9,
	"31 TO 33": 10,
	"34 TO 36": 11,
	"37 TO 39": 12,
	"40 TO 42": 13,
	"43 TO 45": 14,
	"46 TO 48": 15,
	"49 TO 51": 16,
}

var IntToStoreyRange = map[int8]string{
	0:  "01 TO 03",
	1:  "04 TO 06",
	2:  "07 TO 09",
	3:  "10 TO 12",
	4:  "13 TO 15",
	5:  "16 TO 18",
	6:  "19 TO 21",
	7:  "22 TO 24",
	8:  "25 TO 27",
	9:  "28 TO 30",
	10: "31 TO 33",
	11: "34 TO 36",
	12: "37 TO 39",
	13: "40 TO 42",
	14: "43 TO 45",
	15: "46 TO 48",
	16: "49 TO 51",
}

// flat_model column
var FlatModelToInt = map[string]int8{
	"Improved":               0,
	"New Generation":         1,
	"Model A":                2,
	"Simplified":             3,
	"Premium Apartment":      4,
	"Standard":               5,
	"Model A-Maisonette":     6,
	"Apartment":              7,
	"Maisonette":             8,
	"Model A2":               9,
	"Terrace":                10,
	"Adjoined flat":          11,
	"DBSS":                   12,
	"Multi Generation":       13,
	"Premium Maisonette":     14,
	"Improved-Maisonette":    15,
	"Type S1":                16,
	"Type S2":                17,
	"Premium Apartment Loft": 18,
	"2-room":                 19,
	"3Gen":                   20,
}

var IntToFlatModel = map[int8]string{
	0:  "Improved",
	1:  "New Generation",
	2:  "Model A",
	3:  "Simplified",
	4:  "Premium Apartment",
	5:  "Standard",
	6:  "Model A-Maisonette",
	7:  "Apartment",
	8:  "Maisonette",
	9:  "Model A2",
	10: "Terrace",
	11: "Adjoined flat",
	12: "DBSS",
	13: "Multi Generation",
	14: "Premium Maisonette",
	15: "Improved-Maisonette",
	16: "Type S1",
	17: "Type S2",
	18: "Premium Apartment Loft",
	19: "2-room",
	20: "3Gen",
}

// lease_commence_date column
var LeaseCommenceToInt, IntToLeaseCommence = func() (map[string]int8, map[int8]string) {
	startYear := 1966
	endYear := 2022

	yearToInt := map[string]int8{}
	intToYear := map[int8]string{}

	idx := int8(0)
	for year := startYear; year <= endYear; year++ {
		yearToInt[strconv.Itoa(year)] = idx
		intToYear[idx] = strconv.Itoa(year)
		idx++
	}

	return yearToInt, intToYear
}()
