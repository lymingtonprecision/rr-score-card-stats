(ns rrsc.util)

(defn remove-namespaced-keys
  ([m]
   (remove-namespaced-keys m #{}))
  ([m exceptions]
   (apply dissoc m (filter #(and (not (exceptions %))
                                 (some? (namespace %)))
                           (keys m)))))
