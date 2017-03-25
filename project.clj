(defproject recsys "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [flipboard/clojure-hbase "1.0.0-pre"]
                 [yogthos/config "0.8"]]
  :main ^:skip-aot recsys.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {:resource-paths ["config/dev"]}}
  )
