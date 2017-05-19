(ns rrsc.change-event
  (:require [clojure.spec.alpha :as spec]
            [rrsc.change-capture :as change-capture]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; specs


(spec/def
 ::type #{::shop-order-start ::need-date-change})

(spec/def ::order_no string?)
(spec/def ::release_no string?)
(spec/def ::sequence_no string?)

(spec/def
 ::shop-order-reference
 (spec/keys :req-un [::order_no ::release_no ::sequence_no]))

(spec/def ::expected-start ::change-capture/timestamp)
(spec/def ::actual-start ::change-capture/timestamp)

(spec/def ::timestamp ::change-capture/timestamp)
(spec/def ::need-date ::change-capture/timestamp)

(defmulti event-spec first)

(defmethod event-spec ::shop-order-start [_]
  (spec/tuple
   ::type
   (spec/keys :req [::shop-order-reference ::expected-start ::actual-start])))

(defmethod event-spec ::need-date-change [_]
  (spec/tuple
   ::type
   (spec/keys :req [::shop-order-reference ::timestamp ::need-date])))

(spec/def ::event (spec/multi-spec event-spec first))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; shop order start date tracking


(def shop-order-start
  "Transducer:

  * _in_, Change Data Capture messages related to Shop Order changes
  * _out_, Shop Order Start event records, when a change denoting the
    start of a Shop Order is seen

  The produced Shop Order Start events are tuples of:

      [::shop-order-start
       {::shop-order-reference #{:order_no :release_no :sequence_no}
        ::expected-start 'last-known-scheduled-start-date-of-the-order
        ::actual-start   'timestamp-of-change}]

  Retains a dictionary of Shop Order expected start dates (last known
  `revised_start_date`) for all Shop Orders for which an `insert` (or
  `update` containing that field) are seen."
  (fn [xf]
    (let [expected-starts (volatile! {})]
      (fn
        ([] (xf))
        ([rs] (xf rs))
        ([rs ccm]
         (let [k (change-capture/message-key ccm)
               d (change-capture/message-data ccm)
               ts (change-capture/message-timestamp ccm)
               started? (= (:rowstate d) "Started")]
           (some->> d :revised_start_date (vswap! expected-starts assoc k))
           (when (change-capture/is-delete? ccm) (vswap! expected-starts dissoc k))
           (if started?
             (let [expected (change-capture/->timestamp (get @expected-starts k))
                   event [::shop-order-start
                          {::shop-order-reference k
                           ::expected-start expected
                           ::actual-start ts}]]
               (xf rs event))
             rs)))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; shop order need date tracking


(def shop-order-need-date-change
  "Transducer:

  * _in_, Change Data Capture messages related to Shop Order changes
  * _out_, Need Date Change event records, when a change to a Shop Order
    Need Date is seen

  The produced Need Date Change events are tuples of:

      [::need-date-change
       {::shop-order-reference #{:order_no :release_no :sequence_no}
        ::timestamp 'timestamp-of-change
        ::need-date 'new-need-date}]
  "
  (fn [xf]
    (let []
      (fn
        ([] (xf))
        ([rs] (xf rs))
        ([rs ccm]
         (let [k (change-capture/message-key ccm)
               ts (change-capture/message-timestamp ccm)
               nd (some->> (change-capture/message-data ccm)
                           :need_date
                           change-capture/->timestamp)]
           (if (some? nd)
             (xf rs [::need-date-change
                     {::shop-order-reference k
                      ::timestamp ts
                      ::need-date nd}])
             rs)))))))
