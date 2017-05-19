(ns rrsc.change-event-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clj-time.core :as time]
            [clj-time.coerce :as time.coerce]
            [rrsc.generators :refer :all]
            [rrsc.change-event :as ce :refer :all]))

(def ^:dynamic *num-tests* 20)

(def gen-timestamp
  (gen/fmap
   time.coerce/from-long
   (gen/large-integer*
    {:min (time.coerce/to-long (time/plus (time/now) (time/weeks 1)))
     :max (time.coerce/to-long (time/plus (time/now) (time/years 10)))})))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; shop-order-start


(defspec
 shop-order-start-yields-nothing-when-order-not-started
 *num-tests*
 (prop/for-all
  [events (gen/fmap
           (fn [[events expected-start]]
             (assoc-in (vec events) [0 :data :revised_start_date] expected-start))
           (gen/tuple (gen/scale #(max 5 %) gen-shop-order-events)
                      gen-timestamp))]
  (is (empty? (into [] shop-order-start events)))))

(defspec
 shop-order-start-for-a-single-order
 *num-tests*
 (prop/for-all
  [events (gen/scale #(max 5 %) gen-shop-order-events)
   expected-start gen-timestamp]
  (let [shop-order (->> events first :id)
        actual-start (-> events last :info :timestamp)
        events (-> (vec events)
                   (assoc-in [0 :data :revised_start_date] expected-start)
                   (assoc-in [(dec (count events)) :data :rowstate] "Started"))]
    (is (= [[::ce/shop-order-start
             {::ce/shop-order-reference shop-order
              ::ce/expected-start expected-start
              ::ce/actual-start actual-start}]]
           (into [] shop-order-start events))))))

(defspec
 shop-order-start-has-nil-expectation-when-revised-start-date-never-seen
 *num-tests*
 (prop/for-all
  [events (gen/fmap rest (gen/scale #(max 5 %) gen-shop-order-events))]
  (let [n (rand-int (dec (count events)))
        events (assoc-in (vec events) [n :data :rowstate] "Started")
        shop-order (->> events first :id)
        actual-start (get-in events [n :info :timestamp])]
    (is (= [[::ce/shop-order-start
             {::ce/shop-order-reference shop-order
              ::ce/expected-start nil
              ::ce/actual-start actual-start}]]
           (into [] shop-order-start events))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; shop-order-need-date-change

(defspec
 need-date-change-yields-nothing-when-need-date-never-changed
 *num-tests*
 (prop/for-all
  [events (gen/fmap
           (fn [events]
             (map #(update % :data dissoc :need_date) events))
           (gen/scale #(max 5 %) gen-shop-order-events))]
  (is (empty? (into [] shop-order-need-date-change events)))))

(defspec
 need-date-change-yields-every-change
 *num-tests*
 (prop/for-all
  [events (gen/bind
           (gen/scale #(max 5 %) gen-shop-order-events)
           (fn [events]
             (gen/fmap
              (fn [need-dates]
                (reduce
                 (fn [events [n ts]]
                   (assoc-in events [n :data :need_date] ts))
                 (vec events)
                 need-dates))
              (->> (gen/tuple (gen/choose 0 (dec (count events))) gen-timestamp)
                   (gen/list-distinct-by first)
                   gen/not-empty))))]
  (let [shop-order (->> events first :id)
        need-dates (reduce
                    (fn [rs e]
                      (if-let [nd (->> e :data :need_date)]
                        (conj rs [(->> e :info :timestamp) nd])
                        rs))
                    []
                    events)]
    (is (= (mapv
            (fn [[ts nd]]
              [::ce/need-date-change
               {::ce/shop-order-reference shop-order
                ::ce/timestamp ts
                ::ce/need-date nd}])
            need-dates)
           (into [] shop-order-need-date-change events))))))
