package main

import (
	"encoding/csv"
	"log"
	"os"
	"path/filepath"
)

func writeCSVFile(path string, activities Activities) string {
	dir := filepath.Dir(path)
	ensureDirExists(dir)

	file := createCSVfile(path)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writeHeaders(writer)

	for _, activity := range activities {
		writeActivity(writer, activity)
	}

	return path
}

func ensureDirExists(dir string) {
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}
}

func createCSVfile(path string) *os.File {
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	return file
}

func writeHeaders(writer *csv.Writer) {
	headers := []string{
		"current_timestamp",
		"datid",
		"datname",
		"pid",
		"leader_pid",
		"usesysid",
		"usename",
		"application_name",
		"client_addr",
		"client_hostname",
		"client_port",
		"backend_start",
		"xact_start",
		"query_start",
		"query_duration",
		"state_change",
		"wait_event_type",
		"wait_event",
		"state",
		"backend_xid",
		"backend_xmin",
		"query_id",
		"query",
		"backend_type",
	}

	err := writer.Write(headers)
	if err != nil {
		log.Fatalf("Failed to write to CSV: %v", err)
	}
}

func writeActivity(writer *csv.Writer, act Activity) {
	row := []string{
		act.current_timestamp,
		act.datid,
		act.datname,
		act.pid,
		act.leader_pid,
		act.usesysid,
		act.usename,
		act.application_name,
		act.client_addr,
		act.client_hostname,
		act.client_port,
		act.backend_start,
		act.xact_start,
		act.query_start,
		act.query_duration,
		act.state_change,
		act.wait_event_type,
		act.wait_event,
		act.state,
		act.backend_xid,
		act.backend_xmin,
		act.query_id,
		act.query,
		act.backend_type,
	}

	err := writer.Write(row)
	if err != nil {
		log.Fatalf("Failed to write to CSV: %v", err)
	}
}
