(ns rrsc.date
  (:require [clojure.spec.alpha :as spec]
            [clojure.test.check.generators :as gen]
            [clj-time.core :as time]
            [clj-time.coerce :as time.coerce]
            [clj-time.format :as time.format]
            [clj-time.predicates :as time.pred]))

(defn nearest [ts xs]
  (when-let [xs (some-> xs seq sort)]
    (loop [previous  (first xs)
           current   (first xs)
           remainder (rest xs)]
      (cond
       (time/after? current ts) previous
       (empty? remainder) current
       :else (recur current (first remainder) (rest remainder))))))

(defn on-or-before-day? [ts x]
  (let [ts (time/with-time-at-start-of-day ts)
        x  (time/with-time-at-start-of-day x)]
    (or (time/equal? ts x) (time/before? ts x))))

(defn last-weekday-in-month [ts]
  (if (and (time.pred/last-day-of-month? ts) (time.pred/weekday? ts))
    ts
    (let [ldom (time/last-day-of-the-month ts)
          offset (cond
                  (time.pred/saturday? ldom) 1
                  (time.pred/sunday? ldom) 2
                  :else 0)]
      (time/minus ldom (time/days offset)))))

(defn nearest-month-end
  ([]
   (nearest-month-end (time/now)))
  ([ts]
   (let [ts (time/with-time-at-start-of-day ts)
         lwdim (last-weekday-in-month ts)]
     (time/last-day-of-the-month
      (if (or (time/equal? ts lwdim) (time/after? ts lwdim))
        ts
        (time/minus ts (time/months 1)))))))

(defn three-month-period-ending-by
  ([]
   (three-month-period-ending-by (time/now)))
  ([ts]
   (let [ts (time/with-time-at-start-of-day ts)
         e (nearest-month-end ts)
         s (time/first-day-of-the-month
            (time/minus e (time/months 2)))]
     [s e])))

(defn three-months-ago
  ([]
   (three-months-ago (time/now)))
  ([ts]
   (first (three-month-period-ending-by ts))))

(def to-sql time.coerce/to-sql-time)
(def from-sql time.coerce/from-sql-time)

(defn conformer [fmt]
  (let [one-hundred-years-mills (* 100 365 24 60 60 100)
        dt-min (- (System/currentTimeMillis) one-hundred-years-mills)
        dt-max (+ dt-min (* one-hundred-years-mills 2))
        gen #(gen/fmap
              time.coerce/from-long
              (gen/large-integer* {:min dt-min :max dt-max}))
        conf (fn [x]
               (cond
                (instance? org.joda.time.ReadableDateTime x) x
                (instance? org.joda.time.ReadableInstant x) x
                (inst? x) (time.coerce/from-date x)
                (string? x) (try
                             (time.format/parse (time.format/formatter :date-time) x)
                             (catch Exception _
                               ::spec/invalid))
                :else ::spec/invalid))
        unconf #(time.format/unparse fmt %)]
    (spec/with-gen (spec/conformer conf unconf) gen)))
