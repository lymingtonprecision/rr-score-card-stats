(ns rrsc.generators
  (:require [clojure.spec.alpha :as spec]
            [clojure.test.check.generators :as gen]
            [clj-time.core :as time]
            [rrsc.change-capture :as change-capture]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helpers


(def one-week-in-seconds (* 7 24 60 60))

(defn bump-timestamp [ts]
  (let [s (inc (rand-int one-week-in-seconds))]
    (time/plus ts (time/seconds s))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; generic change capture generators


(defn gen-value
  ([]
   (gen/such-that some? (spec/gen (spec/spec ::change-capture/field-value))))
  ([const-or-gen]
   (cond
    (gen/generator? const-or-gen) const-or-gen
    (some? const-or-gen) (gen/return const-or-gen)
    :else (gen-value))))

(defn gen-field-values [fields]
  (apply
   gen/hash-map
   (reduce
    (fn [rs fv]
      (let [[f v] (if (coll? fv)
                    [(first fv) (gen-value (second fv))]
                    [fv (gen-value)])]
        (conj rs f v)))
    []
    fields)))

(defmulti  gen-constraint (fn [c _] c))
(defmethod gen-constraint :table [_ v] (gen/return v))
(defmethod gen-constraint :type  [_ v] (gen/return v))
(defmethod gen-constraint :id    [_ v] (gen-field-values v))
(defmethod gen-constraint :data  [_ v] (gen-field-values v))

(defn gen-ccm
  [constraints]
  (let [ccm-gen (spec/gen (spec/spec ::change-capture/message))
        constraint-gen (->> (reduce
                             (fn [cg [c v]]
                               (assoc cg c (gen-constraint c v)))
                             {}
                             constraints)
                            seq
                            flatten
                            (apply gen/hash-map))]
    (gen/fmap
     (fn [[ccm constraints]]
       (let [ccm (merge ccm constraints)
             insert? (= :insert (:type ccm))
             constrained-id? (seq (:id constraints))]
         (if (and insert? constrained-id?)
           (update ccm :data merge (:id constraints))
           ccm)))
     (gen/tuple ccm-gen constraint-gen))))

(defn randomly-alter [m]
  (let [ks (random-sample 0.5 (keys m))]
    (reduce
     (fn [rs k]
       (assoc rs k (->> (gen/such-that #(not= (get m k) %) (gen-value))
                        (gen/resize 20)
                        gen/sample
                        first)))
     {}
     ks)))

(defn update-from-insert [ins]
  (merge
   ins
   {:type :update
    :info (update (:info ins) :timestamp bump-timestamp)
    :data (randomly-alter (apply dissoc (:data ins) (keys (:id ins))))}))

(defn delete-from-insert [ins]
  (dissoc
   (merge
    ins
    {:type :delete
     :info (update (:info ins) :timestamp bump-timestamp)})
   :data))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; shop order change message generators


(def gen-shop-order-insert
  (gen-ccm
   {:table "ifsapp.shop_ord_tab"
    :type :insert
    :id [[:order_no (gen/not-empty gen/string-ascii)]
         [:release_no "*"]
         [:sequence_no "*"]]}))

(def gen-shop-order-events
  (gen/sized
   (fn [s]
     (gen/fmap
      (fn [ins]
        (let [updates (loop [xs [], prev ins, n 0]
                        (if (= s n)
                          xs
                          (let [upd (update-from-insert prev)
                                xs (conj xs upd)]
                            (recur xs (update upd :data #(merge (:data ins) %)) (inc n)))))]
          (cons ins updates)))
      gen-shop-order-insert))))
