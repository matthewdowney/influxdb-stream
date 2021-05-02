(ns influxdb-stream.core
  (:gen-class)
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


(set! *warn-on-reflection* true)


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
  [{:keys [query query-limit]} start end]
  (let [time-filter (format "time < '%s' AND time >= '%s'"
                            (date-str end) (date-str start))]
    (str
      (string/replace query "$timeFilter" time-filter)
      " LIMIT " query-limit)))


(defn query
  "Execute the `query-string` and return {:columns [...], :data [{...}]}."
  [{:keys [host port db] :as conf} query-string]
  (timbre/trace ">" query-string)
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


(def ^:dynamic *interrupted*
  "A promise which is realized to interrupt data fetching."
  nil)


(defn lazy-data-chunks
  "Create a lazy sequence of parsed `query!` results for data in the intervals.

  If any query fails, the sequence terminates with {::error e :start start}."
  [{:keys [query-limit] :as conf} intervals]
  (lazy-seq
    (if (and *interrupted* (realized? *interrupted*))
      (timbre/info "interrupted while fetching data")
      (when-let [[start end] (first intervals)]
        (try
          (let [ret (query conf (>sql conf start end))
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
            (cons ret (lazy-data-chunks conf next-intervals)))
          (catch Exception e
            (timbre/error e)
            {::error e :start start :end end}))))))


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
            p)))
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

    (timbre/debugf "writing %s rows to %s" (count rows) path)

    (with-open [w (io/writer path)]
      ;; CSV headers
      (.write w (string/join "," columns))
      (.write w "\n")

      ;; CSV lines
      (doseq [r rows]
        (.write w (string/join "," (get-row r)))
        (.write w "\n")))

    (timbre/debugf "wrote %s rows to %s" (count rows) path)))


(defn- await-last-write! [f]
  (when (and f (not (realized? f)))

    ;; Only debug if it's taking some time to complete the previous write
    (when (= (deref f 2500 ::timeout) ::timeout)
      (timbre/debug "awaiting previous write completion..."))

    @f))


(defn- fetch-and-write [conf]
  (timbre/info "fetching column headers and first data chunk...")
  (let [{:keys [columns stream]} (stream-data conf)]
    (timbre/info "got column headers:" columns)
    (loop [stream stream
           last-write-future nil]
      (if-let [nxt (first stream)]
        ;; If there's an error, it's the value which terminates the stream
        (let [?err (-> nxt peek ::error)
              rows-to-write (if ?err (butlast nxt) nxt)]

          ;; If the previous write is still in progress, wait for it to finish
          (await-last-write! last-write-future)

          (when ?err
            (throw (ex-info "query failed" (dissoc (peek nxt) ::error) ?err)))

          ;; Start this write in a new thread
          (let [writing (future (write-to-disk conf columns rows-to-write))]
            (recur (rest stream) writing)))

        (do
          (await-last-write! last-write-future)
          (timbre/info "reached end of data stream"))))))


(defn pull-data
  "Pull data out of InfluxDB in consecutive intervals, writing data in groups
  of some configurable number of rows to a series of CSV files.

  The `stop-promise`, when realized, interrupts the fetching and writing of
  data.

  See `example-conf` for an example configuration map."
  [conf stop-promise]
  (let [?parse-interval #(if (number? %) % (apply enc/ms (reverse %)))
        conf (update conf :interval ?parse-interval)]
    (binding [*interrupted* stop-promise]
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
                  (assoc conf :start start))))))))))


(def example-conf
  "Annotated example configuration."
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


(defonce state nil)


(defn start
  "Pull data out of InfluxDB in consecutive intervals, writing data in groups
  of some configurable number of rows to a series of CSV files. Stops when done,
  or when `stop` is called.

  See `example-conf` for an example configuration map."
  [conf]
  (timbre/info
    (with-out-str
      (println "Starting up with configuration:")
      (pprint/pprint conf)))

  (let [stop-promise (promise)
        state' {:task (future (pull-data conf stop-promise))
                :stop-promise stop-promise}]
    (alter-var-root #'state (constantly state'))
    :ok))


(defn stop
  "Stop the running process, if any."
  []
  (if-let [{:keys [task stop-promise]} state]
    (do
      (deliver stop-promise true)
      (timbre/info "stop signal sent, awaiting completion...")
      @task
      :ok)
    (timbre/info "nothing running")))


(defmacro with-shutdown-hook [[hook-name hook-fn] & body]
  `(let [~hook-name ~hook-fn ;; Make the hook available by name in body
         hook# (Thread. ~hook-name)
         rt# (Runtime/getRuntime)]
     (.addShutdownHook rt# hook#)
     (let [ret# (do ~@body)]
       (.removeShutdownHook rt# hook#)
       ret#)))


(defn -main [& args]
  (let [conf (try
               (timbre/info "Loading config file...")
               (let [conf (read-string (slurp "conf.edn"))]
                 (assert
                   (contains?
                     #{:years :months :weeks :days :hours :mins :secs :msecs :ms}
                     (-> conf :interval second))
                   "config has valid :interval units")
                 conf)
               (catch Exception e
                 (timbre/error e)
                 (timbre/error "Error reading configuration file at conf.edn")
                 (System/exit 1)))]

    ;; Enable granular logging
    (timbre/merge-config! {:min-level :trace})

    ;; Set a hook to try to exit gracefully on Ctrl+C
    (with-shutdown-hook [clean-shutdown
                         (fn []
                           (stop)
                           (timbre/info "graceful shutdown complete"))]

      ;; Start fetching & then await task completion
      (start conf)
      (-> state :task deref)
      (shutdown-agents))))
