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


(defn retrieve-journey-descendants [journey-string]
  "Retrieve all journeys with the same last page"
  (let [journey-vector (string-to-vector journey-string)
        last-page (last journey-vector)]    
    (ds/retrieve-matching-journeys last-page)))




