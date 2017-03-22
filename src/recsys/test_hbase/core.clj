(ns recsys.test-hbase.core
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.util :as hb-util]
            [clojure-hbase.admin :as hb-admin])
  (:gen-class))


(hb/with-table [test (hb/table "test")]
    (hb/query test 
      (hb/get* "row5" :column [:cf :a])))


(hb/put (hb/table "test") "row5" :value [:cf :z "hello"])
(hb/get (hb/table "test") "row5" :column [:cf :a])

(hb/with-table [test (hb/table "test")]
  (hb/as-map (hb/get test "row1" :column [:cf :ca])))


(hb/with-table [test (hb/table "test")]
  (hb/as-map (hb/get test "row5" :column [:cf :a]) :map-family #(keyword (Bytes/toString %)) :map-qualifier #(keyword (Bytes/toString %)) :map-timestamp #(java.util.Date. %) :map-value #(Bytes/toString %) str))


(hb/with-table [test (hb/table "test")]
  (hb/as-map (hb/get test "row1" :column [:cf :a]) :map-family #(keyword (hb-util/as-str %)) :map-qualifier #(keyword (hb-util/as-str %)) :map-timestamp #(java.util.Date. %) :map-value #(hb-util/as-str %) str))




;;     /c
;;   /b
;;  /  \d
;; a  
;;  \c-e


;; create 'context-tree', 'child'
(hb/with-table [ct (hb/table "context-tree")]
  (hb/put ct "a" :value [:child :b "3"])
  (hb/put ct "a" :value [:child :c "2"])
  (hb/put ct "a-b" :value [:child :c "1"])
  (hb/put ct "a-b" :value [:child :d "1"])
  (hb/put ct "a-c" :value [:child :e "1"]))


(hb/with-table [test (hb/table "context-tree")]
  (hb/as-map (hb/get test "a") :map-family #(keyword (hb-util/as-str %)) :map-qualifier #(keyword (hb-util/as-str %)) :map-timestamp #(java.util.Date. %) :map-value #(hb-util/as-str %) str))


(hb/with-table [test (hb/table "context-tree")]
  (hb/as-map (hb/get test "a-b") :map-family #(keyword (hb-util/as-str %)) :map-qualifier #(keyword (hb-util/as-str %)) :map-timestamp #(java.util.Date. %) :map-value #(hb-util/as-str %) str))


(hb/with-table [test (hb/table "context-tree")]
  (hb/as-map (hb/get test "a-b-c") :map-family #(keyword (hb-util/as-str %)) :map-qualifier #(keyword (hb-util/as-str %)) :map-timestamp #(java.util.Date. %) :map-value #(hb-util/as-str %) str))


(hb/with-table [ct (hb/table "context-tree")]
  (hb/scan ct :start-row "a"))


(hb/with-table [ct (hb/table "context-tree")]
  (hb/execute ct 
              (hb/scan* :start-row "a")))

(hb/as-map
 (first
  (hb/with-scanner [ct (hb/table "context-tree")]
    (hb/scan ct :start-row "a"))) :map-family #(keyword (hb-util/as-str %)) :map-qualifier #(keyword (hb-util/as-str %)) :map-timestamp #(java.util.Date. %) :map-value #(hb-util/as-str %) str)


(map (fn [s] (hb/as-map s))
     (hb/with-scanner [ct (hb/table "context-tree")]
       (hb/scan ct :start-row "a")))


(def results
  (map (fn [s] (hb/as-vector s :map-family #(keyword (hb-util/as-str %)) :map-qualifier #(keyword (hb-util/as-str %)) :map-timestamp #(java.util.Date. %) :map-value #(hb-util/as-str %) str))
       (hb/with-scanner [ct (hb/table "context-tree")]
         (hb/scan ct :start-row "a"))))


;; working - returns vector
(map (fn [s] (hb/as-vector s
                           :map-family #(keyword (hb-util/as-str %))
                           :map-qualifier #(keyword (hb-util/as-str %))
                           :map-timestamp #(java.util.Date. %)
                           :map-value #(hb-util/as-str %) str))
     (hb/with-scanner [ct (hb/table "context-tree")]
       (hb/scan ct :start-row "a")))


;; working - returns map
(map (fn [s] (hb/latest-as-map s
                           :map-family #(keyword (hb-util/as-str %))
                           :map-qualifier #(keyword (hb-util/as-str %))
                           :map-value #(hb-util/as-str %) str))
     (hb/with-scanner [ct (hb/table "context-tree")]
       (hb/scan ct :start-row "a-b" :stop-row "a-z")))


;; working - returns keys
(map (fn [r] (hb-util/as-str (.getRow r)))
     (hb/with-scanner [ct (hb/table "context-tree")]
       (hb/scan ct :start-row "a-b" :stop-row "a-z")))


;; working - returns kv - fireing 2 queries?
(zipmap
 (map (fn [r] (hb-util/as-str (.getRow r)))
      (hb/with-scanner [ct (hb/table "context-tree")]
        (hb/scan ct :start-row "a-b" :stop-row "a-z")))
 (map (fn [s] (hb/latest-as-map s
                                :map-family #(keyword (hb-util/as-str %))
                                :map-qualifier #(keyword (hb-util/as-str %))
                                :map-value #(hb-util/as-str %) str))
      (hb/with-scanner [ct (hb/table "context-tree")]
        (hb/scan ct :start-row "a-b" :stop-row "a-z"))))


;; working, 1 query. Check fmap to avoid list of maps
(map (fn [s]
       (hash-map
        (hb-util/as-str (.getRow s))
        (hb/latest-as-map s
                           :map-family #(keyword (hb-util/as-str %))
                           :map-qualifier #(keyword (hb-util/as-str %))
                           :map-value #(hb-util/as-str %) str)))
      (hb/with-scanner [ct (hb/table "context-tree")]
        (hb/scan ct :start-row "a-b" :stop-row "a-z")))


;; Working, single hashmap, 1 query
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


;; working add journey
(defn add-journey [parent page]

  (def page_kw (keyword page))
  
  (def current-value
    (get
     (get
      (hb/with-table [ct (hb/table "context-tree")]
        (hb/latest-as-map (hb/get ct parent :column [:child page_kw])
                          :map-family #(keyword (hb-util/as-str %))
                          :map-qualifier #(keyword (hb-util/as-str %))
                          :map-value #(hb-util/as-str %) str))
      :child)
     page_kw))

  (def new-value
    (-> current-value
        (#(if (nil? %) 0 (Integer. %)))
        (inc)
        (str)))
  
  (hb/with-table [ct (hb/table "context-tree")]
    (hb/put ct parent :value [:child page_kw new-value])))


;; working
(defn add-journey [parent page]
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
