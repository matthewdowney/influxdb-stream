(defproject influxdb-stream "0.2.0"
  :description "Pull data out of InfluxDB in chunks and write to CSV files."
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/data.json "2.4.0"]
                 [clj-http "3.12.3"]
                 [com.taoensso/timbre "6.0.1"]]
  :jvm-opts ["-server" "-Xms5G" "-Xmx10G"]
  :repl-options {:init-ns influxdb-stream.core}
  :aot :all
  :main influxdb-stream.core)
