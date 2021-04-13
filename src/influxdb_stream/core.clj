(ns influxdb-stream.core
  (:require [clojure.string :as string]
            [clojure.set :as set]
            [clojure.pprint :as pprint]
            [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.data.json :as json]

            [clj-http.client :as client]
            [taoensso.encore :as enc]
            [taoensso.timbre :as timbre])
  (:import (java.text SimpleDateFormat DateFormat)
           (java.util TimeZone Date)
           (clojure.lang TransformerIterator RT IteratorSeq)))


(def ^:private ^ThreadLocal thread-local-iso-date-format
  (proxy [ThreadLocal] []
    (initialValue []
      (doto (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        (.setTimeZone (TimeZone/getTimeZone "GMT"))))))


(defn inst-iso-str
  "The ISO 8601 UTC timestamp for some inst.

  E.g. 2021-01-23T16:25:15.932Z"
  [i]
  (let [^DateFormat utc-format (.get thread-local-iso-date-format)
        ^Date d (if (instance? Date i) i (Date. ^long (inst-ms i)))]
    (.format utc-format d)))


(defn date-str
  "If passed a string, returns it as-is, otherwise tries to format a
  java.util.Date."
  [d]
  (if (string? d) d (inst-iso-str d)))


(defn >sql
  "A query for all rows for `measurement` between the `start` and `end` insts.

  Optionally, pass strings instead of insts."
  [measurement start end limit]
  (let [date (fn [x] (if (string? x) x (date-str x)))]
    (format
      "SELECT * FROM %s WHERE time < '%s' AND time >= '%s' LIMIT %s"
      measurement (date end) (date start) limit)))


(defn query
  "Execute the `query-string` and return {:columns [...], :data [{...}]}."
  [{:keys [host port db] :as conf} query-string]
  (let [resp (client/post
               (format "http://%s:%s/query" host port)
               {:query-params {:q query-string :db db}})]

    (when-not (= (:status resp) 200)
      (throw (ex-info "failed" resp)))

    (let [{:keys [results]} (json/read-str (:body resp) :key-fn keyword)
          {:keys [columns values]} (-> results first :series first)]
      {:columns columns
       :data (mapv (fn [v] (zipmap columns v)) values)})))


(defn time-intervals
  "A sequence of `[start-inst, end-inst]` for intervals between the provided
  start and end times."
  [start-inst interval-ms end-inst]
  (let [end-ms (inst-ms end-inst)]
    (->> (inst-ms start-inst)
         (iterate (partial + interval-ms))
         (take-while #(<= % end-ms))
         (map #(Date. ^long %))
         (partition 2 1))))


(defn unchunk [s]
  (lazy-seq
    (when-let [x (first (seq s))]
      (cons x (unchunk (rest s))))))


(defn lazy-data-chunks
  "Create a lazy sequence of parsed `query!` results for data in the intervals.

  If any query fails, the sequence terminates with {::error e :start start}."
  [{:keys [measurement query-limit] :as conf} intervals]
  (lazy-seq
    (when-let [[start end] (first intervals)]
      (try
        (let [ret (query conf (>sql measurement start end query-limit))
              ;; If the query hit the limit for returned data, try a new
              ;; interval from end of returned data to end of requested interval
              next-intervals (if (= (count (:data ret)) query-limit)
                               (cons
                                 ;; Same end time, but start time is the last
                                 ;; data point
                                 [(-> ret :data peek (get "time"))
                                  end]
                                 (rest intervals))
                               (rest intervals))]
          (timbre/debugf "got %s rows for chunk from %s to %s ..."
                         (count (:data ret))
                         (date-str start)
                         (date-str end))
          (cons ret (unchunk (lazy-data-chunks conf next-intervals))))
        (catch Exception e
          (timbre/error e)
          {::error e :start start :end end})))))


(defn lazy-sequence
  "Like clojure.core/sequence, except completely lazy (no chunking)."
  [xform coll]
  (IteratorSeq/create (TransformerIterator/create xform (RT/iter coll))))


(defn stream-data
  "Return {:columns [_] :stream <LazySeq>} where :columns includes the ordered
  columns for the _first_ chunk of data, and :stream is a lazy sequence of
  vectors of rows."
  [{:keys [start interval end rows-per-file] :as conf}]
  (let [stream (->> (time-intervals start interval end)
                    (lazy-data-chunks conf)
                    ;; Immediately advance the stream until the first chunk with
                    ;; row data.
                    (drop-while #(and (empty? (:data %)) (not (::error %)))))
        cols (-> stream first :columns)
        stream (lazy-sequence
                 (comp
                   (mapcat
                     (fn [chunk]
                       ;; Leave ::error values intact, otherwise mapcat the
                       ;; :data
                       (if (::error chunk)
                         [chunk]
                         (:data chunk))))
                   (partition-all rows-per-file))
                 stream)]
    {:columns cols :stream stream}))


(defn all-columns
  "Keep the `base-columns` vector in order, adding any additional column names
  present in rows at the end."
  [base-columns rows]
  (let [all-columns (into #{} (mapcat keys) rows)
        extras (set/difference all-columns (set base-columns))]
    (into base-columns (vec extras))))


(defn unique-path [file date-format first-row-ts]
  ;; Try to read the first row timestamp as a date and format it, otherwise just
  ;; use the literal timestamp value.
  (let [date-str (try
                   (.format
                     (SimpleDateFormat. date-format)
                     (inst/read-instant-date first-row-ts))
                   (catch Exception _
                     first-row-ts))
        path (format file date-str)]
    (if (.exists (io/file path))
      (loop [n 1]
        (let [p (str path "." n)]
          (if (.exists (io/file p))
            (recur (inc n))
            (do
              (timbre/warn "intended path conflicts with an existing file!")
              (timbre/warnf "writing to %s instead" p)
              p))))
      path)))


(defn write-to-disk [{:keys [file date-format]} columns rows]
  (let [columns (all-columns columns rows)
        get-cell (fn [col]
                   (fn [row]
                     (let [cell (get row col)]
                       (cond
                         (number? cell) cell
                         (nil? cell) "\"\""
                         :else (pr-str cell)))))
        get-row (apply juxt (map get-cell columns))
        path (unique-path file date-format (-> rows first (get "time")))]
    (io/make-parents path)
    (with-open [w (io/writer path)]
      ;; CSV headers
      (.write w (string/join "," columns))
      (.write w "\n")

      ;; CSV lines
      (doseq [r rows]
        (.write w (string/join "," (get-row r)))
        (.write w "\n")))
    (timbre/debugf "wrote %s rows to %s" (count rows) path)))


(defn- fetch-and-write [{:keys [rows-per-file] :as conf}]
  (timbre/info "fetching column headers and first data chunk...")
  (let [{:keys [columns stream]} (stream-data conf)]
    (loop [stream stream]
      (if-let [nxt (first stream)]
        ;; If there's an error, it's the value which terminates the stream.
        (let [?err (-> nxt peek ::error)
              rows-to-write (if ?err (butlast nxt) nxt)]

          (write-to-disk conf columns rows-to-write)

          (when ?err
            (throw (ex-info "query failed" (dissoc (peek nxt) ::error) ?err)))

          (recur (rest stream)))

        (timbre/info "reached end of data stream")))))


(defn pull-data
  "Pull data out of InfluxDB in consecutive intervals, writing data in groups
  of some configurable number of rows to a series of CSV files.

  See `example-conf` for an example configuration map."
  [conf]
  (let [?parse-interval #(if (number? %) % (apply enc/ms (reverse %)))
        conf (update conf :interval ?parse-interval)]
    (try
      (fetch-and-write conf)
      (catch Exception e
        (timbre/error e)
        (when-let [start (-> e ex-data :start)]
          (println "\n***\n")
          (timbre/debugf "query failed starting at %s" (date-str start))
          (timbre/info
            (with-out-str
              (println "To retry, picking up from where you left off, try:")
              (pprint/pprint
                (assoc conf :start start)))))))))


(def example-conf
  "Annotated example configuration."
  {;; The InfluxDB database to connect to
   :host  "127.0.0.1"
   :port  8086
   :db    "marketdata"


   ;; Fetch all rows for this measurement, between the start and end dates,
   ;; making queries spanning :interval amounts of time. The :interval is
   ;; important because it imposes a bound on InfluxDB memory usage for a
   ;; single query.
   :measurement "trade"
   :start #inst"2020-01-01"
   :interval [24 :hours]
   :end #inst"2020-02-01"

   ;; Write a certain number of rows per file to a series of files named with
   ;; the given pattern, which accepts the timestamp of the first row.
   :file "rspread.%s.csv"
   :rows-per-file 10000})
