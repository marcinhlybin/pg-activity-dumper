package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

const (
	updateAfterSeconds = 10
	dumpAfterSeconds   = 300
	dumpPath           = "/var/log/pg_activity"
)

const (
	host     = "/var/run/postgresql"
	user     = "postgres"
	port     = 5432
	password = ""
	dbname   = "postgres"
	sslmode  = "disable"
)

// Time formats
const (
	YYYYMMDD = "2006-01-02"
	HHMM     = "1504"
)

func main() {
	var activities = make(Activities)

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", host, port, user, password, dbname, sslmode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	updateActivities(db, activities)

	updateTicker := time.NewTicker(time.Duration(updateAfterSeconds) * time.Second)
	dumperTicker := time.NewTicker(time.Duration(dumpAfterSeconds) * time.Second)

	for {
		select {
		case <-updateTicker.C:
			updateActivities(db, activities)

		case <-dumperTicker.C:
			exportToCSV(activities)

			// Clear the activities map after dumping
			activities = make(Activities)
		}
	}
}
