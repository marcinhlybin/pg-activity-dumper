package main

import (
	"database/sql"
	"log"
)

type Activity struct {
	id                string // added field
	current_timestamp string // added field
	datid             string
	datname           string
	pid               string
	leader_pid        string
	usesysid          string
	usename           string
	application_name  string
	client_addr       string
	client_hostname   string
	client_port       string
	backend_start     string
	xact_start        string
	query_start       string
	query_duration    string // added field
	state_change      string
	wait_event_type   string
	wait_event        string
	state             string
	backend_xid       string
	backend_xmin      string
	query_id          string
	query             string
	backend_type      string
}

type Activities map[string]Activity

const pgActivityStatQuery = `
	SELECT
		md5(
			COALESCE(pid::text, 'NULL') ||
			COALESCE(backend_start::text, 'NULL') ||
			COALESCE(query_start::text, 'NULL') ||
			COALESCE(client_addr::text, 'NULL') ||
			COALESCE(client_port::text, 'NULL')
		) as id,
		CURRENT_TIMESTAMP AS current_timestamp,
		COALESCE(datid::text, 'NULL') AS datid,
		COALESCE(datname::text, 'NULL') AS datname,
		COALESCE(pid::text, 'NULL') AS pid,
		COALESCE(leader_pid::text, 'NULL') AS leader_pid,
		COALESCE(usesysid::text, 'NULL') AS usesysid,
		COALESCE(usename::text, 'NULL') AS usename,
		COALESCE(application_name::text, 'NULL') AS application_name,
		COALESCE(client_addr::text, 'NULL') AS client_addr,
		COALESCE(client_hostname::text, 'NULL') AS client_hostname,
		COALESCE(client_port::text, 'NULL') AS client_port,
		COALESCE(backend_start::text, 'NULL') AS backend_start,
		COALESCE(xact_start::text, 'NULL') AS xact_start,
		COALESCE(query_start::text, 'NULL') AS query_start,
		COALESCE(EXTRACT(EPOCH FROM (now() - query_start))::text, 'NULL') AS query_duration,
		COALESCE(state_change::text, 'NULL') AS state_change,
		COALESCE(wait_event_type::text, 'NULL') AS wait_event_type,
		COALESCE(wait_event::text, 'NULL') AS wait_event,
		COALESCE(state::text, 'NULL') AS state,
		COALESCE(backend_xid::text, 'NULL') AS backend_xid,
		COALESCE(backend_xmin::text, 'NULL') AS backend_xmin,
		COALESCE(query_id::text, 'NULL') AS query_id,
		COALESCE(query::text, 'NULL') AS query,
		COALESCE(backend_type::text, 'NULL') AS backend_type
	FROM pg_stat_activity
`

func updateActivities(db *sql.DB, activities Activities) {
	rows, err := db.Query(pgActivityStatQuery)
	if err != nil {
		log.Fatalf("Failed to query pg_stat_activity: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		act, err := getActivityRecord(rows)
		if err != nil {
			log.Fatal(err)
		}
		updateActivityRecord(act, activities)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Failed to process rows: %v", err)
	}
}

func updateActivityRecord(act Activity, activities Activities) {
	if act.state_change != activities[act.id].state_change {
		activities[act.id] = act
	}
}

func getActivityRecord(rows *sql.Rows) (Activity, error) {
	var act Activity

	err := rows.Scan(
		&act.id,
		&act.current_timestamp,
		&act.datid,
		&act.datname,
		&act.pid,
		&act.leader_pid,
		&act.usesysid,
		&act.usename,
		&act.application_name,
		&act.client_addr,
		&act.client_hostname,
		&act.client_port,
		&act.backend_start,
		&act.xact_start,
		&act.query_start,
		&act.query_duration,
		&act.state_change,
		&act.wait_event_type,
		&act.wait_event,
		&act.state,
		&act.backend_xid,
		&act.backend_xmin,
		&act.query_id,
		&act.query,
		&act.backend_type)

	return act, err
}
