# influxdb-stream

InfluxDB has some memory usage issues which make it difficult to 
[read](https://community.influxdata.com/t/export-big-data-from-influxdb/2204/3) 
or [write](https://github.com/influxdata/influxdb/issues/15433) large amounts of 
data at once. 

This repository contains helpers to split these operations up into "streams" of 
finely chunked operations which don't cause InfluxDB to self-destruct.

## Usage with a config file

1. Download the tool (alternatively, build it from source with `lein uberjar`).
   ```
   $ wget blahblahblah
   ```

2. Create a configuration file describing the data to pull and the size of each
   query in the stream in a file called `conf.edn`.
   ```clojure
   ; conf.edn
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
   :date-format   "YYYY-MM-dd"
   :file          "trade.%s.csv"
   :rows-per-file 10000}
   ```
   
   To prevent queries from causing InfluxDB to use up all the available RAM: 
   (1) pick a `:query-limit` that is relatively small, 20k has worked well for 
   me; and (2) tweak the `:interval` that the query spans so that approximately 
   :query-limit data points are returned anyway, depending on how dense your 
   data is. You can specify e.g. `[15 :secs]`, `[15 :mins]`, `[1.5 :hours]`, or 
   `[7 :days]`. 
   

3. Run the tool, specifying with `-Xmx` how much RAM is available for use. E.g.
   to run with 10G of RAM allocated:
   ```
   $ java -Xmx10G -jar influxdb-stream.jar
   ```
   

## Usage from clojure

```clojure 
(start
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
   :date-format   "YYYY-MM-dd"
   :file          "trade.%s.csv"
   :rows-per-file 10000})
```
