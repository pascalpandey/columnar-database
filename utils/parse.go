package utils

import (
	"flag"
	"fmt"
	"os"
	"strconv"
)

func ParseFlags() (int, int, string, string) {
	matric := flag.String("matric", "", "Matriculation number for data query")
	rawData := flag.String("data", "", "File location of raw data")
	flag.Parse()

	// Parse matric number
	if *matric == "" || len(*matric) < 9 {
		fmt.Println("Please provide a valid matric number using -matric flag")
		os.Exit(1)
	}

	year, err := strconv.Atoi(string((*matric)[len(*matric)-2]))
	if err != nil {
		fmt.Println("Could not parse year")
		os.Exit(1)
	}
	if year >= 4 && year <= 9 {
		year += 2010
	} else {
		year += 2020
	}

	month, err := strconv.Atoi(string((*matric)[len(*matric)-3]))
	if err != nil {
		fmt.Println("Could not parse month")
		os.Exit(1)
	}
	if month == 0 {
		month = 10
	}

	townInt, err := strconv.Atoi(string((*matric)[len(*matric)-3]))
	if err != nil {
		fmt.Println("Could not parse town")
		os.Exit(1)
	}
	town := IntToTown[int8(townInt)]

	fmt.Printf("Query:\n")
	fmt.Printf("- Time range: %d-%02d to %d-%02d\n", year, month, year, month+1)
	fmt.Printf("- Town: %s\n", town)
	fmt.Printf("- Area: ≥ 80m²\n")

	// Parse raw data file location
	if *rawData == "" {
		fmt.Println("Please provide a file path using -data")
		os.Exit(1)
	}

	if _, err := os.Stat(*rawData); os.IsNotExist(err) {
		fmt.Printf("File does not exist: %s\n", *rawData)
		os.Exit(1)
	} else if err != nil {
		fmt.Printf("Error checking file: %v\n", err)
		os.Exit(1)
	}

	return year, month, town, *rawData
}
