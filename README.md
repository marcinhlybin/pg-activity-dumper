# PostgreSQL Activity Dumper

Gets records from pg_stat_activity view every 10 seconds and dumps the data to CSV file every 5 minutes to `/var/log/pg_activity/YYYY-MM-DD/YYYY-MM-DD_HHMM.csv.gz` for future analysis.

Build with:

```
GOOS=linux GOARCH=amd64 go build
```
