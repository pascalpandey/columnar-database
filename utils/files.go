package utils

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

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