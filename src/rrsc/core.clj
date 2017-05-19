(ns rrsc.core
  (:require [rrsc.date :as date]
            [rrsc.change-event :as change-event]))

(defn collect-start-dates
  ([rs] rs)
  ([rs [event-type e]]
   (if-not (= event-type ::change-event/shop-order-start)
     rs
     (let [so (::change-event/shop-order-reference e)
           expected (::change-event/expected-start e)
           actual (::change-event/actual-start e)]
       (update rs so merge {:started [expected actual]})))))

(defn collect-need-dates
  ([rs] rs)
  ([rs [event-type e]]
   (if-not (= event-type ::change-event/need-date-change)
     rs
     (let [so (::change-event/shop-order-reference e)
           ts (::change-event/timestamp e)
           nd (::change-event/need-date e)]
       (assoc-in rs [so :need-dates ts] nd)))))

(defn need-date-as-of
  [need-date-log ts]
  (when (seq need-date-log)
    (get need-date-log (date/nearest ts (keys need-date-log)))))
