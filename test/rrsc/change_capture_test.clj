(ns rrsc.change-capture-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [rrsc.generators :refer :all]
            [rrsc.change-capture :refer :all]))

(def ^:dynamic *num-tests* 20)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; hydrate-records


(defspec
 hydrate-records-from-insert
 *num-tests*
 (prop/for-all
  [l (gen/list-distinct-by :id gen-shop-order-insert {:min-elements 1})]
  (let [events (into [] hydrate-records l)]
    (is (= events (mapv (fn [ccm] [ccm nil (:data ccm)]) l))))))

(deftest
  hydrate-records-with-deletion
  (let [ins (first (gen/sample (gen/resize 20 gen-shop-order-insert) 1))
        del (delete-from-insert ins)]
    (is (= (into [] hydrate-records [ins del])
           [[ins nil (:data ins)]
            [del (:data ins) nil]]))))

(defspec
  hydrate-records-as-sequence-of-states
  *num-tests*
  (prop/for-all
   [events gen-shop-order-events]
   (let [states (reductions merge (map :data events))
         butlast-state (->> states reverse (drop 1) first)
         last-state (last states)
         act (into [] hydrate-records events)]
     (is (= (count act) (count events)))
     (is (= (first act) [(first events) nil (first states)]))
     (is (= (last act)  [(last events) butlast-state last-state])))))
