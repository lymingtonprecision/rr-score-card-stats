(ns user
  (:require [clojure.pprint :refer [pprint]]
            [com.stuartsierra.component :as component]
            [reloaded.repl
             :refer [system init start stop clear go reset reset-all]]))

(defn dev-system []
  {})

(reloaded.repl/set-init! #'user/dev-system)

(comment
 (reset)
 (reset-all)
 (clear)
 (init)
 (start)
 (stop)
 (pprint system))
