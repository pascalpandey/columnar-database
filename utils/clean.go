package utils

import (
	"os"
	"path/filepath"
)

func CleanDir(dir string) error {
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		return os.Remove(path)
	})
	return err
}