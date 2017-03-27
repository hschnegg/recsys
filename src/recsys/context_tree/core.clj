(ns ^{:doc "Implement the main features of the Context Tree recommender"
      :author "Herve Schnegg"}

    recsys.context-tree.core

  (:require [config.core :refer [env]]
            [recsys.context-tree.data-store :as ds]
            [clojure.string :as str]))


(defn process-journey [journey]
  "Break a string containing a journey into a map of page and reversed parent"
  (let [pages (str/split journey (re-pattern (str "\\" (:journey-separator (:context-tree env)))))]
    (-> pages
        (#(if (:hash-page-name? (:context-tree env))
            (map hash %1)
            %1))
        (reverse)
        (#(hash-map
           :page (first %1)
           :parent (str/join (:journey-separator (:context-tree env)) (rest %1)))))))



(defn add-journey [journey]
  "Add a journey stored in a string into the CT data store"
  (let [processed-journey (process-journey journey)
        page (:page processed-journey)
        parent (:parent processed-journey)]
    (when (not= parent ""
                (ds/store-journey parent page)))))
