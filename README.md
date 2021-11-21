# influxdb-stream

InfluxDB has some memory usage issues which make it difficult to 
[read](https://community.influxdata.com/t/export-big-data-from-influxdb/2204/3) 
or [write](https://github.com/influxdata/influxdb/issues/15433) large amounts of 
data at once. 

This repository contains helpers to split these operations up into "streams" of 
finely chunked operations which don't cause InfluxDB to self-destruct.

## Install
Create an influxdb-stream/ directory and download the idb.jar tool
(alternatively, build it from source with `lein uberjar`).
```
mkdir influxdb-stream 
wget -O influxdb-stream/idb.jar https://github.com/matthewdowney/influxdb-stream/releases/latest/download/idb.jar
```

## Query data and write to CSV(s)

1. Create a configuration file describing the data to pull and the size of each
   query in the stream in a file called `conf.edn`.
   ```clojure
   ; ~/influxdb-stream/conf.edn
   {;; The InfluxDB database to connect to
   :host          "127.0.0.1"
   :port          8086
   :db            "marketdata"

   ;; Fetch all rows for this measurement, between the start and end dates,
   ;; making queries spanning :interval amounts of time. The :interval is
   ;; important because it imposes a bound on InfluxDB memory usage for a
   ;; single query. The $timeFilter is replaced with a time range expression
   ;; according to where in the time range the cursor is, and a LIMIT is
   ;; appended to the query.
   :query         "SELECT * FROM trade WHERE $timeFilter"
   :query-limit   20000 ; max rows returned per query
   :start         #inst"2020-01-01"
   :end           #inst"2020-02-01"
   :interval      [24 :hours]

   ;; Write a certain number of rows per file to a series of files named with
   ;; the given pattern, which accepts the timestamp of the first row.
   :date-format   "yyyy-MM-dd"
   :file          "trade.%s.csv"
   :rows-per-file 10000}
   ```
   
   To prevent queries from causing InfluxDB to use up all the available RAM: 
   (1) pick a `:query-limit` that is relatively small, 20k has worked well for 
   me; and (2) tweak the `:interval` that the query spans so that approximately 
   :query-limit data points are returned anyway, depending on how dense your 
   data is. You can specify e.g. `[15 :secs]`, `[15 :mins]`, `[1.5 :hours]`, or 
   `[7 :days]`. 
   

2. Run the tool, specifying with `-Xmx` how much RAM is available for use. E.g.
   to run with 10G of RAM allocated:
   ```
   $ cd influxdb-stream
   $ java -Xmx10G -jar idb.jar read-to-csv conf.edn
   ```
   
## Write data via repeated queries

This is useful for e.g. sideloading data from a backup 
(see [sideloading instructions from influx docs](https://docs.influxdata.com/influxdb/v1.7/administration/backup_and_restore/#restore-examples)
and GitHub issue for [error which occurs if instructions are followed](https://github.com/influxdata/influxdb/issues/15433))
or for [downsampling](https://docs.influxdata.com/influxdb/v1.7/guides/downsampling_and_retention/) 
data.

More generally, this is useful anywhere you have a `SELECT ... INTO` query.

1. Create a `conf.edn` file specifying the query and a time range.
   ```clojure
   ; ~/influxdb-stream/conf.edn
   {;; The InfluxDB database to connect to
   :host          "127.0.0.1"
   :port          8086
   :db            "marketdata"

   ;; Execute the query first for the time range [start, start + 60 mins], then
   ;; for [start + 60 mins, start + 120 mins], and so on.
   :start         #inst"2020-01-01"
   :end           #inst"2021-01-01"
   :interval      [60 :mins]

   ;; Run this query to downsample measurements from "ticker" into
   ;; the "downsampled-ticker", which takes the last ask and bid values for
   ;; each minute. The $timeFilter is replaced with a time range expression.
   :query         "SELECT last(ask) AS \"ask\", last(bid) AS \"bid\"
                  INTO \"downsampled-ticker\"
                  FROM \"ticker\"
                  WHERE $timeFilter
                  GROUP BY time(1m), \"exchange\", \"market\" fill(none)"}
   ```
2. Run the tool. Note here that the JVM doesn't need any special RAM allocation,
   since the memory-intensive computation happens in Influx.
   ```
   cd influxdb-stream
   java -jar idb.jar run-queries conf.edn
   ```
