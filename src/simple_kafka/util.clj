(ns simple-kafka.util
  (:require [taoensso.timbre :as log]
            [config.core :as config]
            [clojure.java.shell :as shell]))

;;==================Shell approach===============
(defn find-offsets-by-group-id
  [group-id]
  (let [results (shell/sh "bin/kafka-consumer-groups.sh" "-bootstrap-server" (config/env :kafka-bootstrap-servers) "-describe" "-group" group-id
                          :dir (config/env :kafka-command-line-dir))
        offsets (:out results)
        error (:err results)]
    (log/debugf "offsets by topic: %s \nfor group-id %s" offsets group-id)
    (log/debugf "offsets error message: %s \nfor group-id %s" error group-id)))


;;==================AdminClient approach: not working===============

(defn -main [& args]
  (println "Running kafka util")
  (if (< (count args) 2)
    (throw (IllegalArgumentException. "Missing parameters"))
    (println "args: " args))
  (let [tool (first args)]
    (println (format "running tool(%s)" tool))
    (case tool
      "find-offsets-by-group-id" (find-offsets-by-group-id  (nth args 1))
      (throw (IllegalArgumentException. (format "Illegal tool name: %s%n" tool)))))
  (println "Done kafka util"))
