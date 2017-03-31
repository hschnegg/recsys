(ns ^{:doc "Implement the main features of the Context Tree recommender"
      :author "Herve Schnegg"}

    recsys.context-tree.core

  (:require [config.core :refer [env]]
            [recsys.context-tree.data-store :as ds]
            [clojure.string :as str]))


(defn string-to-vector [journey-string]
  "Transform a journey defined as a separated string into a vector of its pages"
  (str/split journey-string (re-pattern (str "\\" (:journey-separator (:context-tree env))))))


(defn vector-to-string [journey-vector]
  "Transform a journey stored as a vector of pages into a separated string"
  (str/join (:journey-separator (:context-tree env)) journey-vector))


(defn process-parent [parent-journey-vector]
  "Reverse the journey and hash its elements if required"
  (let [reversed-parent (reverse parent-journey-vector)]
    (if (:hash-page-name? (:context-tree env))
      (map hash reversed-parent)
      (identity reversed-parent))))


(defn add-journey [journey-string]
  "Add a journey stored in a string into the CT data store"
  (let [journey-vector (string-to-vector journey-string)
        page (last journey-vector)
        parent-vector (process-parent (pop journey-vector))]
    (when (> (count parent-vector) 0)
      (ds/store-journey (vector-to-string parent-vector) page))))


(defn retrieve-journey-descendants-0 [journey-string]
  "Retrieve all journeys with the same last page"
  (let [journey-vector (string-to-vector journey-string)
        last-page (last journey-vector)]    
    (ds/retrieve-matching-journeys last-page)))


(defn retrieve-journey-family [last-page]
  "Retrieve all journeys with the same last page"
    (ds/retrieve-matching-journeys last-page))


(defn suffix-weights [journey-length]
  "Calculate the weighting to apply to recs generated from different suffixes"
  (let [powers (range journey-length)
        alpha-powered (map #(Math/pow (:alpha (:context-tree env)) %) powers)
        weight-full-journey (/ 1 (reduce + alpha-powered))]
    (map #(* % weight-full-journey) alpha-powered)))


(defn build-all-suffixes
  "Derive all the journey's suffixes"
  ([reversed-journey-vector]
   (build-all-suffixes reversed-journey-vector []))
  ([reversed-journey-vector suffixes]
   (if (empty? reversed-journey-vector)
     suffixes
     (recur (pop reversed-journey-vector) (conj suffixes reversed-journey-vector)))))        


(def family (retrieve-journey-family "c"))
(def suffixes (map vector-to-string (build-all-suffixes ["c" "b" "a"])))
(def k (keys family))
(def s "c")


(map #(str/starts-with? % s)c k) ;; key starts with s? 
(map (fn [k1] (reduce (fn [e1 e2] (or e1 e2)) (map (fn [s] (= k1 s)) suffixes))) k) ;; key belongs to suffix
(map #(= s %) k) ;; key = s?
