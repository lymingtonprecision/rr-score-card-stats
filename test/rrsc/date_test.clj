(ns rrsc.date-test
  (:require [clojure.test :refer [testing deftest is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clj-time.core :as time]
            [clj-time.coerce :as time.coerce]
            [clj-time.predicates :as time.pred]
            [rrsc.date :refer :all]))

(def gen-end-of-month
  (gen/fmap
   #(apply time/last-day-of-the-month %)
   (gen/tuple (gen/choose 1 9999) (gen/choose 1 12))))

(defn gen-date-upto-day [n]
  (gen/fmap
   (fn [[y m d]]
     (time/date-time y m (min d (time/number-of-days-in-the-month y m))))
   (gen/tuple (gen/choose 1 9999) (gen/choose 1 12) (gen/choose 1 n))))

(def gen-date (gen-date-upto-day 31))

(def gen-time-since-epoch
  (let [epoch (time.coerce/to-long (time/epoch))]
    (gen/sized
     (fn [s]
       (gen/fmap
        (fn [n]
          (time.coerce/from-long (+ epoch (* n 1000))))
        gen/int)))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; nearest


(deftest nearest-nothing-is-nil
  (is (nil? (nearest (time/now) []))))

(defspec
 nearest-single-element-is-that-element
 (prop/for-all
  [d1 gen-time-since-epoch
   d2 gen-time-since-epoch]
  (is (= d1 (nearest d2 [d1])))))

(defspec
 nearest-returns-exact-match-when-present
 (prop/for-all
  [d gen-date
   vs (gen/not-empty (gen/list-distinct gen/s-pos-int))]
  (let [r (->> vs
               (map
                #(if (zero? (rand-int 1))
                   (time/minus d (time/seconds %))
                   (time/plus d (time/seconds %))))
               (cons d)
               shuffle)]
    (is (= d (nearest d r))))))

(defspec
 nearest-returns-nearest
 (prop/for-all
  [xs (gen/not-empty (gen/list-distinct gen-time-since-epoch))]
  (let [ts (rand-nth xs)
        near (time/plus ts (time/millis 1))]
    (is (= ts (nearest near xs))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; on-or-before-day?


(defspec
 on-or-before-day
 (prop/for-all
  [d gen-date]
  (is (on-or-before-day? d (time/plus d (time/days 1))))
  (is (on-or-before-day? d (time/with-time-at-start-of-day d)))
  (is (on-or-before-day? d (.withTime d 12 0 0 0)))
  (is (on-or-before-day? d (.withTime d 23 59 59 999)))
  (is (not (on-or-before-day? d (time/minus d (time/days 1)))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; last-weekday-in-month


(defspec
 last-weekday-in-month-given-end-of-the-month
 (prop/for-all
  [eom gen-end-of-month]
  (is (= (last-weekday-in-month eom)
         (cond
          (time.pred/weekday?  eom) eom
          (time.pred/saturday? eom) (time/minus eom (time/days 1))
          (time.pred/sunday?   eom) (time/minus eom (time/days 2)))))))

(defspec
 last-weekday-in-month-given-day-during-month
 (prop/for-all
  [d (gen-date-upto-day 26)]
  (let [eom (time/last-day-of-the-month d)]
    (is (time/within? (time/minus eom (time/days 2))
                      eom
                      (last-weekday-in-month d))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; nearest-month-end


(defspec
 nearest-month-end-given-weekday-end-of-month-is-same-month
 (prop/for-all
  [eom (gen/such-that time.pred/weekday? gen-end-of-month)]
  (is (= (nearest-month-end eom) eom))))

(defspec
 nearest-month-end-given-month-ending-in-weekend-is-same-month
 (prop/for-all
  [d (gen/fmap
      (fn [d]
        (if (and (time.pred/sunday? d) (even? (rand-int 2)))
          (time/minus d (time/days 1))
          d))
      (gen/such-that time.pred/weekend? gen-end-of-month 100))]
  (is (= (nearest-month-end d) (time/last-day-of-the-month d)))))

(defspec
 nearest-month-end-given-date-prior-to-end-is-previous-month
 (prop/for-all
  [d (gen/fmap
      (fn [d]
        (let [ldbme (- (time/number-of-days-in-the-month d) 4)]
          (if (> (time/day d) ldbme)
            (time/minus d (time/days (- (time/day d) ldbme)))
            d)))
      gen-date)]
  (is (= (nearest-month-end d)
         (time/last-day-of-the-month (time/minus d (time/months 1)))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; three-month-period-ending-by


(defspec
 three-month-period-is-three-months-from-nearest-month-end
 (prop/for-all
  [nme (gen/fmap nearest-month-end gen-date)]
  (is (= (time/in-months (apply time/interval (three-month-period-ending-by nme)))
         2 ;; 0 start, +1 middle month, +1 end month = 2, not 3!
         ))))
