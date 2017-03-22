(ns recsys.context-tree.data-store
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.util :as hb-util]
            [clojure-hbase.admin :as hb-admin]))


;; create 'context-tree', 'child'


(defn add-journey [parent page]
  "Update the visit counter for an existing journey or create a new journey in the CT"
  (let [page-kw (keyword page)]
    (-> page-kw
        ;; Retrieve current value from CT in HBase
        ((fn [p-kw] (p-kw (:child
                           (hb/with-table [ct (hb/table "context-tree")]
                             (hb/latest-as-map (hb/get ct parent :column [:child p-kw])
                                               :map-family #(keyword (hb-util/as-str %))
                                               :map-qualifier #(keyword (hb-util/as-str %))
                                               :map-value #(hb-util/as-str %) str))))))
        ((fn [current-value] (if (nil? current-value) 0 (Integer. current-value))))
        (inc)
        (str)
        ;; Write incremented (or new) value to CT in HBase
        ((fn [new-value] (hb/with-table [ct (hb/table "context-tree")]
                           (hb/put ct parent :value [:child page-kw new-value])))))))


(into {}
      (map (fn [s]
             (hash-map
              (hb-util/as-str (.getRow s))
              (hb/latest-as-map s
                                :map-family #(keyword (hb-util/as-str %))
                                :map-qualifier #(keyword (hb-util/as-str %))
                                :map-value #(hb-util/as-str %) str)))
           (hb/with-scanner [ct (hb/table "context-tree")]
             (hb/scan ct :start-row "a"))))
