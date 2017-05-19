(defproject rrsc "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]

                 ;; general
                 [aero "1.1.2"]
                 [clj-time "0.13.0"]
                 [net.cgrand/xforms "0.9.2"]

                 ;; kafka
                 [org.apache.kafka/kafka-clients "0.10.2.0"]
                 [cheshire "5.7.0"]

                 ;; database
                 [org.clojure/java.jdbc "0.6.1"]
                 [org.clojars.zentrope/ojdbc "11.2.0.3.0"]
                 [yesql "0.5.3"]]

  :exclusions [org.clojure/clojurescript]

  :profiles {:dev {:source-paths ["dev" "test"]
                   :dependencies [[reloaded.repl "0.2.3"]
                                  [org.clojure/test.check "0.9.0"]]}}

  :repl-options {:init-ns user})
