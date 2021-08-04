(defproject tendant/simple-kafka-client "0.4.4"
  :description "Not another kafka client libary, instead this is a collection of functions for building kafka client in Clojure."
  :url "https://github.com/tendant/simple-kafka-client"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.apache.kafka/kafka-clients "2.7.0"]
                 [org.clojure/data.json "2.4.0"]
                 [cheshire "5.10.0"]
                 [yogthos/config "1.1.8"]]
  :aliases {"utils" ["run" "-m" "simple-kafka.util"]})
