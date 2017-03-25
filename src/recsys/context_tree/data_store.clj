(ns recsys.context-tree.data-store
  (:require [config.core :refer [env]]
            [clojure-hbase.core :as hb]
            [clojure-hbase.util :as hb-util]
            [clojure-hbase.admin :as hb-admin]))


;; create 'context-tree', 'child'


(defn add-journey [parent page]
  "Update the visit counter for an existing journey or create a new journey in the CT"
  (let [page-kw (keyword page)]
    (-> page-kw
        ;; Retrieve current value from CT in HBase
        ((fn [p-kw] (p-kw ((:children-cf (:context-tree (:datastore env)))
                           (hb/with-table [ct (hb/table (:table (:context-tree (:datastore env))))]
                             (hb/latest-as-map (hb/get ct parent :column [(:children-cf (:context-tree (:datastore env))) p-kw])
                                               :map-family #(keyword (hb-util/as-str %))
                                               :map-qualifier #(keyword (hb-util/as-str %))
                                               :map-value #(hb-util/as-str %) str))))))
        ((fn [current-value] (if (nil? current-value) 0 (Integer. current-value))))
        (inc)
        (str)
        ;; Write incremented (or new) value to CT in HBase
        ((fn [new-value] (hb/with-table [ct (hb/table (:table (:context-tree (:datastore env))))]
                           (hb/put ct parent :value [(:children-cf (:context-tree (:datastore env))) page-kw new-value])))))))


(defn retrieve-matching-journeys [last-page]
  "Retrieve all journeys with the same last page"
  (into {}
        (map (fn [scanner]
               (hash-map
                ;; retrieve keys (parents)
                (hb-util/as-str (.getRow scanner))
                ;; retrieve children details
                (hb/latest-as-map scanner
                                  :map-family #(keyword (hb-util/as-str %))
                                  :map-qualifier #(keyword (hb-util/as-str %))
                                  :map-value #(hb-util/as-str %) str)))
             (hb/with-scanner [ct (hb/table (:table (:context-tree (:datastore env))))]
               (hb/scan ct :start-row last-page :stop-row (str last-page "zzz"))))))
  
  


