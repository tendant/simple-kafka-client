(ns simple-kafka.util
  (:require [taoensso.timbre :as log]
            [config.core :as config]
            [clojure.java.shell :as shell]
            [clojure.string :as string]))

;;==================Shell approach===============
(defn find-offsets-by-group-id
  [group-id]
  (let [results (shell/sh "bin/kafka-consumer-groups.sh"
                          "-bootstrap-server" (config/env :kafka-bootstrap-servers)
                          "-describe" "-group" group-id
                          :dir (config/env :kafka-command-line-dir))
        output (:out results)
        error (:err results)]
    (log/debugf "offsets: %s \nfor group-id: %s" output group-id)
    (log/debugf "offsets error message: %s \nfor group-id: %s" error group-id)))

(defn find-offsets-by-group-and-topic
  [group-id topic]
  (let [results (shell/sh "bin/kafka-consumer-groups.sh"
                          "-bootstrap-server" (config/env :kafka-bootstrap-servers)
                          "-describe" "-group" group-id
                          :dir (config/env :kafka-command-line-dir))
        _ (log/debugf "offsets error message: %s \nfor group-id: %s" (:err results) group-id)
        output (:out results)
        assignments (->> output
                         (re-find #"TOPIC\s+PARTITION\s+CURRENT-OFFSET\s+LOG-END-OFFSET\s+LAG\s+CONSUMER-ID\s+HOST\s+CLIENT-ID\s+((.*\n|$)*)")
                         second)
        _ (println "assignments:" assignments)
        error (->> output
                   (re-find #"(?m)^Error:.*$"))]
    (if (or (not (not-empty assignments))
            (not-empty error))
      (log/errorf "find assignments: %s with error: %s" assignments error)
      (if topic
        (let [topic-partitions (->> (string/split-lines assignments)
                                    (filter #(.contains % topic)))]
          (log/debugf "offsets: %s \nfor group-id: %s and topic: %s" (string/join "\n" topic-partitions) group-id topic))
        (log/debugf "offsets: %s \nfor group-id: %s" output group-id)))))

(defn reset-offset-for-group-topic-and-partitions
  [to-offset group-id topic partitions-comma-str]
  (let [topic-partitions (if-not topic
                           ["-all-topics"]
                           (if partitions-comma-str
                             ["-topic" (str topic ":" partitions-comma-str)]
                             ["-topic" topic]))
        command (-> ["bin/kafka-consumer-groups.sh"
                     "-bootstrap-server" (config/env :kafka-bootstrap-servers)
                     "-group" group-id
                     "-reset-offsets" "-to-offset" (str to-offset)]
                    (concat topic-partitions))
        _ (println "command:" command)
        prepared (shell/with-sh-dir (config/env :kafka-command-line-dir) (apply shell/sh command))
        output (:out prepared)
        _ (log/debugf "prepared reset offsets: %s \nfor group-id: %s\ntopic: %s\npartitions-comma-str: %s" output group-id topic partitions-comma-str)
        assignments (->> output
                         (re-find #"TOPIC\s+PARTITION\s+NEW-OFFSET\s+((.*\n|$)*)")
                         second)
        _ (println "assignments:" assignments)
        error (->> output
                   (re-find #"(?m)^Error:.*$"))]
    (if (or (not (not-empty assignments))
            (not-empty error))
      (log/errorf "reset offset assignments: %s with error: %s" assignments error)
      (let [executed (shell/with-sh-dir (config/env :kafka-command-line-dir) (apply shell/sh command "-execute"))]
        (log/debugf "executed reset offsets: %s \nfor group-id: %s\ntopic: %s\npartitions-comma-str: %s" (:out executed) group-id topic partitions-comma-str)))))

;;==================AdminClient approach: not working===============

(defn -main [& args]
  (println "Running kafka util")
  (if (< (count args) 2)
    (throw (IllegalArgumentException. "Missing parameters"))
    (println "args: " args))
  (let [tool (first args)]
    (println (format "running tool(%s)" tool))
    (case tool
      "find-offsets" (do
                       (println "required: group-id\noptional: topic")
                       (case (count args)
                         2 (find-offsets-by-group-and-topic (nth args 1) nil)
                         3 (find-offsets-by-group-and-topic (nth args 1) (nth args 2))
                         (throw (IllegalArgumentException. "Invalid parameters"))))
      "reset-offset" (do
                       (println "required: to-offset, group-id\noptional: topic, comma-seperated-partitions")
                       (case (count args)
                         3 (reset-offset-for-group-topic-and-partitions (nth args 1) (nth args 2) nil nil)
                         4 (reset-offset-for-group-topic-and-partitions (nth args 1) (nth args 2) (nth args 3) nil)
                         5 (reset-offset-for-group-topic-and-partitions (nth args 1) (nth args 2) (nth args 3) (nth args 4))
                         (throw (IllegalArgumentException. "Invalid parameters"))))
      (throw (IllegalArgumentException. (format "Illegal tool name: %s%n" tool)))))
  (println "Done kafka util"))
