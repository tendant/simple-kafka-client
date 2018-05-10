(defproject simple-kafka-client "0.1.3-SNAPSHOT"
  :description "Not another kafka client libary, instead this is a collection of functions for building kafka client in Clojure."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/kafka-clients "0.11.0.0"]
                 [simple-timbre-kafka-appender "0.1.0"]
                 [org.clojure/data.json "0.2.6"]
                 [yogthos/config "0.9"]
                 [proto-repl "0.3.1"]]
  :aliases {"utils" ["run" "-m" "simple-kafka.util"]})
