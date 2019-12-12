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

;; serializer in org.apache.kafka.common.serialization:
;; https://kafka.apache.org/082/javadoc/org/apache/kafka/common/serialization/package-summary.html
;;
;; ByteArrayDeserializer
;;
;; ByteArraySerializer
;;
;; StringDeserializer String encoding defaults to UTF8 and can be
;; customized by setting the property key.deserializer.encoding,
;; value.deserializer.encoding or deserializer.encoding.
;;
;; StringSerializer String encoding defaults to UTF8 and can be
;; customized by setting the property key.serializer.encoding,
;; value.serializer.encoding or serializer.encoding.

(defn- make-consumer
  ([bootstrap-servers group-id opts]
   (-> (merge default-kafka-consumer-properties
              opts
              {"bootstrap.servers" bootstrap-servers
               "group.id" group-id
               "enable.auto.commit" "false"
               "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
               "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})
       (make-properties)
       (KafkaConsumer.)))
  ([bootstrap-servers group-id]
   (make-consumer bootstrap-servers group-id nil))) ;; json-deserializer json-deserializer)))

(defn make-producer [bootstrap-servers]
  (-> {"bootstrap.servers" bootstrap-servers
       "acks" "all"
       "linger.ms" "1"
       "buffer.memory" "4194304"
       "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
       "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"}
      (make-properties)
      (KafkaProducer.))) ;; json-serializer json-serializer)))

(def ^:private utf-8 (java.nio.charset.Charset/forName "utf-8"))

(defn deserialize
  "Deserialize bytes data as JSON, when failure, it will log as error and return nil"
  [^bytes data]
  (if (and data (pos? (alength data)))
    (-> (ByteArrayInputStream. data)
        (InputStreamReader. utf-8)
        (json/read :key-fn keyword))))

(defn serialize [obj]
  (let [baos (ByteArrayOutputStream.)]
    (with-open [w (OutputStreamWriter. baos utf-8)]
      (json/write obj w))
    (.toByteArray baos)))

(defn send-record [producer topic-name k v]
  (if v
    (->> v ;(serialize v)
         (ProducerRecord. topic-name k)
         (.send producer))))

(defn start-job
  ([bootstrap-servers group-id from-topic to-topic error-topic process-fn ex-fn opts]
   (let [consumer (make-consumer bootstrap-servers group-id opts)
         producer (make-producer bootstrap-servers)
         topics [from-topic]]
     ;; from-topic is required
     ;;
     ;; to-topic is not required
     ;;
     ;; error-topic is required for non-error handling job, it should
     ;; be null for error handling job to prevent possible infinite
     ;; loop.
     ;;
     (log/info "start-job topics:" from-topic, to-topic, error-topic)
     (try
       (log/info "start-job subscribing...")
       (.subscribe consumer topics)
       (log/info "start-job subscribed!")
       (while true
         (let [^ConsumerRecords records (.poll consumer Long/MAX_VALUE)]
           (log/debug "start-job poll...")
           (when-not (.isEmpty records)
             (log/debug "start-job found records!")
             (doseq [^ConsumerRecord record records]
               (let [topic (.topic record)
                     partition (.partition record)
                     offset (.offset record)
                     key (.key record)
                     value (.value record) ; (deserialize (.value record))
                     ]
                 (log/debug "start-job start processing...")
                 (try
                   (when-let [result (process-fn key value)]
                     (if to-topic
                       (send-record producer to-topic nil result)
                       (log/warn "Process function returned a result, to-topic is not setup yet. This might be a bug!")))
                   (catch Exception ex
                     (log/errorf ex "Failed processing group: %s, topic: %s, partition: %s, offset: %s, value: %s." group-id topic partition offset value)
                     ;; Forward record to error-topic
                     ;; TODO: add error information and possible retry count
                     (if error-topic
                       (send-record producer error-topic key (json/write-str {:group-id group-id
                                                                              :from-topic from-topic
                                                                              :to-topic to-topic
                                                                              :error-topic error-topic
                                                                              :key key
                                                                              :value value}))
                       (throw (ex-info "error-topic is not configured. For non-error handling job error-topic is required. For error handling job, error-topic should be nil and please fix the issue which caused exception."
                                       {:exception ex})))))))
             ;;; IMPORTANT: Only commit offset when all records are done.
             (.commitSync consumer))))
       (catch Exception ex
         (log/error ex "caught exception, processing topics:%s." topics)
         (if ex-fn
           (ex-fn ex)))
       (finally
         (.unsubscribe consumer)
         (System/exit 1))))))
