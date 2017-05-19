(ns scratch
  (:require [clj-time.core :as time]
            [net.cgrand.xforms :as xforms]
            [rrsc.kafka :as kafka]
            [rrsc.ifs :as ifs]
            [rrsc.date :as date]
            [rrsc.change-capture :as change-capture]
            [rrsc.change-event :as change-event]
            [rrsc.config :as config :refer [config]]
            [rrsc.core :as rrsc]))

(def cfg (config))
(def db-spec (config/db-spec cfg))

;; collated start and need dates, by shop order
#_
(def sosn
  (with-open [c (kafka/ccm-consumer (config/kafka-broker cfg))]
    (transduce
     (comp (map #(.value %))
           (xforms/multiplex
            [change-event/shop-order-start
             change-event/shop-order-need-date-change]))
     (completing
      (fn [rs e] (-> rs
                     (rrsc/collect-start-dates e)
                     (rrsc/collect-need-dates e))))
     {}
     (kafka/topic->seq c (config/shop-order-change-topic cfg)))))

;; on time launch, by month, last three months
#_
(let [rrsos (set (ifs/rr-shop-order-ids db-spec))
      has-start-stat? (fn [[_ v]] (:started v))
      is-rr-so? (fn [[so _]] (rrsos so))
      last-three-months (date/three-month-period-ending-by)
      within-three-months? (fn [[_ v]]
                             (let [exp (-> v :started first)]
                               (time/within? (first last-three-months)
                                             (last last-three-months)
                                             exp)))]
  (transduce
   (comp (filter has-start-stat?)
         (filter is-rr-so?)
         (filter within-three-months?))
   (completing
    (fn [rs [_ v]]
      (let [[exp act] (map time/with-time-at-start-of-day (:started v))
            month (time/first-day-of-the-month exp)
            on-time (or (time/before? act exp) (time/equal? act exp))]
        (update-in rs [month on-time] #(inc (or % 0))))))
   {}
   sosn))

;; lead time adherence, by month, last three months
#_
(let [rrsos (set (ifs/rr-shop-order-ids db-spec))
      rrsor (ifs/rr-receipts-since db-spec (date/to-sql (date/three-months-ago)))]
  (reduce
   (fn [rs receipt]
     (let [so (select-keys receipt #{:order_no :receipt_no :sequence_no})
           ts (-> receipt :date_created date/from-sql time/with-time-at-start-of-day)
           sc (sosn so)
           need-dates (:need-dates sc)
           nd (if need-dates
                (rrsc/need-date-as-of need-dates ts)
                (date/from-sql (:need_date receipt)))
           month (time/first-day-of-the-month ts)
           qty (receipt :qty_received)
           within-leadtime (when nd (or (time/before? ts nd) (time/equal? ts nd)))]
       (update-in rs [month within-leadtime] #(+ (or % 0M) qty))))
   {}
   rrsor))
