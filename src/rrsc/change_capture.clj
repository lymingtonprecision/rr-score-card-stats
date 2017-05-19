(ns rrsc.change-capture
  (:require [clojure.spec.alpha :as spec]
            [clojure.test.check.generators :as gen]
            [clj-time.format :as time.format]
            [rrsc.date :as date]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; spec


(def type-codes #{:insert :update :delete})

(spec/def
 ::type
 (spec/with-gen
  (spec/conformer
   #(or (type-codes (keyword %)) ::spec/invalid)
   #(name %))
  #(gen/elements type-codes)))

(spec/def ::table string?)
(spec/def ::field-name (spec/with-gen keyword? (fn [] gen/keyword)))

(spec/def
 ::field-value
 (spec/with-gen
  #(or (nil? %) (string? %) (number? %))
  #(gen/frequency
    [[1 (gen/return nil)]
     [6 gen/string-ascii]
     [3 gen/pos-int]])))

(spec/def ::id   (spec/map-of ::field-name ::field-value))
(spec/def ::data (spec/map-of ::field-name ::field-value))

(spec/def ::user_id string?)
(spec/def ::timestamp (date/conformer (time.format/formatters :date-time)))
(spec/def ::rowid string?)
(spec/def ::trans_id string?)
(spec/def ::trans_seq (spec/and int? pos?))

(spec/def
 ::info
 (spec/keys :req-un [::user_id ::timestamp]
            :opt-un [::rowid ::trans_id ::trans_seq]))

(spec/def
 ::message
 (spec/keys :req-un [::table ::type ::info ::id]
            :opt-un [::data]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic record inspection/value extraction


(defn message-key       [ccm] (ccm :id))
(defn message-type      [ccm] (ccm :type))
(defn message-data      [ccm] (ccm :data))
(defn message-timestamp [ccm] (-> ccm :info :timestamp))

(defn is-insert? [ccm] (= (ccm :type) :insert))
(defn is-update? [ccm] (= (ccm :type) :update))
(defn is-delete? [ccm] (= (ccm :type) :delete))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; conversion/conform'ing fns


(defn ->timestamp [s]
  (let [ts (spec/conform ::timestamp s)]
    (when (not= ::spec/invalid ts)
      ts)))

(spec/fdef
 ->timestamp
 :args (spec/cat :in any?)
 :ret (spec/or :ok ::timestamp :error nil?))

(defn ->message [xs]
  (let [ccm (spec/conform ::message xs)]
    (when (not= ::spec/invalid ccm)
      ccm)))

(spec/fdef
 ->message
 :args (spec/cat :in map?)
 :ret (spec/or :ok ::message :error nil?))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; record recomposition


(def hydrate-records
  "Transducer:

  * _in_ Change Capture Messages
  * _out_ vectors of `[m old-value new-value]`

  Where:

  * `m` is the Change Capture Message
  * `old-value` is the last known value of the record identified in `m`, if any
  * `new-value` is the new value of the record

  Given an:

  * `:insert`, will return `[m nil (:data m)]`
  * `:update`, will return `[m last-known-value (merge last-known-value (:data m))]`
  * `:delete`, will return `[m last-known-value nil]`"
  (fn [xf]
    (let [records (volatile! {})]
      (fn
        ([] (xf))
        ([rs] (xf rs))
        ([rs r]
         (let [k (message-key r)
               data (message-data r)
               old-rec (get @records k)
               new-rec (when-not (is-delete? r) (merge old-rec data))]
           (if (some? new-rec)
             (vswap! records assoc k new-rec)
             (vswap! records dissoc k))
           (xf rs [r old-rec new-rec])))))))
