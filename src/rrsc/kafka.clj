(ns rrsc.kafka
  (:require [clojure.java.io :as io]
            [cheshire.core :as cheshire]
            [rrsc.change-capture :as change-capture])
  (:import [java.io ByteArrayInputStream]
           [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization Deserializer]))

(def ^:dynamic *poll-timeout* 5000)

(defn json-deserializer []
  (reify Deserializer
    (close [_])
    (configure [_ _ _])
    (deserialize [_ topic payload]
      (with-open [ba (ByteArrayInputStream. payload)
                  r (io/reader ba)]
        (cheshire/parse-stream r true nil)))))

(defn ccm-deserializer []
  (reify Deserializer
    (close [_])
    (configure [_ _ _])
    (deserialize [_ topic payload]
      (with-open [is (ByteArrayInputStream. payload)
                  r (io/reader is)]
        (some-> (cheshire/parse-stream r true nil)
                change-capture/->message)))))

(defn ccm-consumer
  [broker]
  (KafkaConsumer.
   {"bootstrap.servers" broker
    "auto.offset.reset" "earliest"
    "enable.auto.commit" "false"}
   (json-deserializer)
   (ccm-deserializer)))

(defn partitions [^KafkaConsumer consumer topic-name]
  (mapv #(TopicPartition. (.topic %) (.partition %))
        (.partitionsFor consumer topic-name)))

(defn topic->seq
  [^KafkaConsumer consumer topic-name]
  (let [ps (partitions consumer topic-name)]
    (doto consumer
      (.assign ps)
      (.seekToBeginning ps))
    (let [poll (fn poll []
                 (let [rs (.poll consumer *poll-timeout*)]
                   (when-not (.isEmpty rs)
                     (lazy-cat rs (lazy-seq (poll))))))]
      (poll))))
