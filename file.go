package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

func exportToCSV(activities Activities) {
	path := getAbsolutePath(dumpPath, "csv")
	writeCSVFile(path, activities)
	err := gzipFile(path)
	if err != nil {
		log.Fatalf("Failed to compress file: %v", err)
	}
}

func getAbsolutePath(baseDir string, ext string) string {
	now := time.Now()
	dateStr := now.Format(YYYYMMDD)
	timeStr := now.Format(HHMM)

	subDir := dateStr
	filename := fmt.Sprintf("%s_%s.%s", dateStr, timeStr, ext)
	path := filepath.Join(baseDir, subDir, filename)

	return path
}

func gzipFile(srcPath string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destPath := srcPath + ".gz"
	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	writer := gzip.NewWriter(destFile)

	_, err = io.Copy(writer, srcFile)
	if err != nil {
		writer.Close()
		os.Remove(destPath)
		return err
	}

	// Remove source file if no errors
	return os.Remove(srcPath)
}
