package utils

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sc4023/data"
	"strings"
)

// used to clean column_store directory on each run
func CleanDir(dir string) error {
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Failed to delete directory: %v\n", err)
			return err
		}
		if info.IsDir() {
			return nil
		}
		return os.Remove(path)
	})
	return err
}

// count byte width of csv header files, used in column store initialization
func CountHeaderByte(filePath string) int {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		fmt.Printf("Failed to read line: %v\n", err)
	}

	return len(line)
}

// save final results to a file
func SaveResults(matric string, month int8, town int8, area float64, results []float64) {
	filePath := fmt.Sprintf("results/ScanResult_%s.csv", matric)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		fmt.Printf("failed to create directory %s: %s\n", dir, err)
		return
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("failed to open file %s: %s\n", filePath, err)
		return
	}

	fmt.Printf("Result:\n")
	categories := []string{"Minimum Price", "Average Price", "Standard Deviation of Price", "Minimum Price per Square Meter"}
	for i := 0; i<4; i++ {
		fmt.Printf("- %s: %.2f\n", categories[i], results[i])
	}

	writer := csv.NewWriter(file)
	writer.Write([]string{"Year", "Month", "Town", "Category", "Value"})
	for i := 0; i<4; i++ {
		date := strings.Split(data.IntToMonth[month], "-")
		writer.Write([]string{date[0], date[1], data.IntToTown[town], categories[i], fmt.Sprintf("%.2f", results[i])})
	}
	writer.Flush()

	fmt.Printf("Results saved to: %s", filePath)
}
