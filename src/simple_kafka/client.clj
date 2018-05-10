(ns simple-kafka.client
  (:require [taoensso.timbre :as log]
            [clojure.data.json :as json])
  (:import (java.util Properties)
           (java.io ByteArrayInputStream
                    ByteArrayOutputStream
                    InputStreamReader
                    OutputStreamWriter)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig ConsumerRecord ConsumerRecords)))

(defn- make-properties [map]
  (log/debug "Properties:" map)
  (doto (Properties.) (.putAll map)))

(def ^:private default-kafka-consumer-properties
  {;; DO NOT USE. if use, be aware, long timeout will delay broker
   ;; detects client error and delay partition rebalance
   "session.timeout.ms" "10000"

   ;; request.timeout.ms must always be larger than
   ;; max.poll.interval.ms because this is the maximum time that a
   ;; JoinGroup request can block on the server while the consumer is
   ;; rebalancing
   "request.timeout.ms" "31000"

   "max.poll.records" "5"

   ;; Maximum allowed time for a batch to complete processing and
   ;; commit offset. While working on long running job, try to
   ;; increase `max.poll.interval.ms` and decrease `max.poll.records`,
   ;; `max.poll.interval.ms` is restricted by `request.timeout.ms`
   ;; configuration in kafka brokers.
   ;; https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread
   "max.poll.interval.ms" "30000"

   ;; What to do when there is no initial offset in Kafka or if an offset is out of range:
   ;; earliest: automatically reset the offset to the earliest offset
   ;; latest: automatically reset the offset to the latest offset
   ;; none: throw exception to the consumer if no previous offset is found or the consumer's group
   ;; anything else: throw exception to the consumer.
   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"

})

(defn- make-consumer
  ([bootstrap-servers kafka-group-id opts]
   (-> (merge default-kafka-consumer-properties
              opts
              {"bootstrap.servers" bootstrap-servers
               "group.id" kafka-group-id
               "enable.auto.commit" "false"
               "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
               "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"})
       (make-properties)
       (KafkaConsumer.)))
  ([bootstrap-servers kafka-group-id]
   (make-consumer bootstrap-servers kafka-group-id nil))) ;; json-deserializer json-deserializer)))

(defn make-producer [bootstrap-servers]
  (-> {"bootstrap.servers" bootstrap-servers
       "acks" "all"
       "linger.ms" "1"
       "buffer.memory" "4194304"
       "key.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer"
       "value.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer"}
      (make-properties)
      (KafkaProducer.))) ;; json-serializer json-serializer)))

(def ^:private utf-8 (java.nio.charset.Charset/forName "utf-8"))

(defn deserialize
  "Deserialize bytes data as JSON, when failure, it will log as error and return nil"
  [^bytes data]
  (if (and data (pos? (alength data)))
    (try
      (-> (ByteArrayInputStream. data)
          (InputStreamReader. utf-8)
          (json/read :key-fn keyword))
      (catch Exception e
        (log/error e "Failed deserialize data:" data)))))

(defn serialize [obj]
  (let [baos (ByteArrayOutputStream.)]
    (with-open [w (OutputStreamWriter. baos utf-8)]
      (json/write obj w))
    (.toByteArray baos)))

(defn send-record [producer topic-name k v]
  (if v
    (->> (serialize v)
         (ProducerRecord. topic-name k)
         (.send producer))))

(defn start-job
  ([bootstrap-servers kafka-group-id topic-name process-fn ex-fn opts]
   (let [consumer (make-consumer bootstrap-servers kafka-group-id opts)
         topics (cond
                  (string? topic-name) [topic-name]
                  (coll? topic-name) topic-name)]
     (log/info "start-job topics:" topics (type topics))
     (try
       (.subscribe consumer topics)
       (while true
         (let [^ConsumerRecords records (.poll consumer Long/MAX_VALUE)]
           (when-not (.isEmpty records)
             (doseq [^ConsumerRecord record records
                     :let [topic (.topic record)
                           offset (.offset record)
                           key (.key record)
                           value (deserialize (.value record))]]
               (try
                 (process-fn key value)
                 (catch Exception ex
                   (log/errorf ex "Failed processing group: %s, topic: %s, offset: %s, value: %s." kafka-group-id topic offset value)
                   (throw (ex-info "Failed processing record:" {:group kafka-group-id
                                                                :topic topic
                                                                :offset offset
                                                                :record record})))))
             (.commitSync consumer))))
       (catch Exception ex
         (log/error ex "caught exception, processing topics:%s." topics)
         (if ex-fn
           (ex-fn ex)))
       (finally
         (.unsubscribe consumer)
         (System/exit 1)))))
  ([bootstrap-servers kafka-group-id topic-name process-fn]
   (start-job bootstrap-servers kafka-group-id topic-name process-fn nil nil))
  ([bootstrap-servers kafka-group-id topic-name process-fn opts]
   (start-job bootstrap-servers kafka-group-id topic-name process-fn nil opts)))
