(defproject influxdb-stream "0.1.0"
  :description "Pull data out of InfluxDB in chunks and write to CSV files."
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "0.2.6"]
                 [clj-http "3.9.1"]
                 [com.taoensso/timbre "5.1.0"]]
  :repl-options {:init-ns influxdb-stream.core})
