(ns rrsc.config
  (:require [clojure.spec.alpha :as spec]
            [aero.core :as aero]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; spec


(spec/def
 ::non-empty-string
 (spec/and string? #(not (empty? %))))

(spec/def ::kafka-broker ::non-empty-string)
(spec/def ::shop-order-change-topic ::non-empty-string)

(spec/def ::host ::non-empty-string)
(spec/def ::sid ::non-empty-string)
(spec/def ::user ::non-empty-string)
(spec/def ::password ::non-empty-string)

(spec/def
 ::ifs
 (spec/keys :req-un [::host ::sid ::user ::password]))

(spec/def
 ::config
 (spec/keys
  :req-un [::kafka-broker ::shop-order-change-topic ::ifs]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; fns


(defn config []
  (let [cfg (aero/read-config "resources/config.edn")]
    (if-let [err (spec/explain-data ::config cfg)]
      (throw (ex-info "invalid config" err))
      cfg)))

(defn kafka-broker [cfg]
  (cfg :kafka-broker))

(defn shop-order-change-topic [cfg]
  (cfg :shop-order-change-topic))

(defn db-spec
  [cfg]
  (let [{:keys [host sid user password]} (cfg :ifs)]
    {:dbtype "oracle:thin"
     :dbname sid
     :database (str host "/" sid)
     :user user
     :password password}))
