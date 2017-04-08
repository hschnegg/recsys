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


(defn retrieve-subtree [last-page]
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
     (recur (drop-last reversed-journey-vector) (conj suffixes (vector-to-string reversed-journey-vector))))))        


(defn get-matching-journeys [suffix subtree-parents]
  "Get tree journeys starting by suffix"
  (filter (fn [parent] (str/starts-with? parent suffix)) subtree-parents))


(defn abs-to-perc [vector]
  "Transform a vector of values into percentages"
  (map #(/ % (reduce + vector)) vector))


(defn retrieve-pages-visits [parent tree]
  "Retrieve the children pages and counts from tree"
  (let [pages-kw (keys (:child (get tree parent)))
        pages (map name pages-kw)
        visits-str (vals (:child (get tree parent)))
        visits (map #(Integer. %) visits-str)]
    (zipmap pages visits)))
        

(defn get-suffix-children [suffix weight tree]
  "Retrieve all the matching journeys (pages and visits) for a given suffix"
  (let [children (->> [suffix]
                      ;; extract matching journeys
                      (mapcat #(get-matching-journeys % (keys tree)))
                      (distinct)
                      (sort-by count)
                      (reverse)
                      ;; retrieve children and visits
                      (map #(retrieve-pages-visits % tree))
                      ;; aggregate resulting hashmap
                      (apply merge-with +))
        pages (keys children)
        visits (map #(* % weight) (abs-to-perc (vals children)))]
    (zipmap pages visits)))


(defn iterate-over-suffixes
  "Retrieve recommendations for provided suffixes"
  ([weighted-suffixes tree]
   (iterate-over-suffixes weighted-suffixes tree {}))
  ([weighted-suffixes tree recs]
   (let [first-weighted-suffix (first weighted-suffixes)
         first-suffix (first first-weighted-suffix)
         weight (last first-weighted-suffix)]
     (if (empty? first-suffix)
       (reverse (sort-by val recs))
       (recur (rest weighted-suffixes) (dissoc tree first-suffix) (merge-with + recs (get-suffix-children first-suffix weight tree)))))))


(defn retrieve-recommendations [journey-string]
  "Prepare journey and call recommendations"
  (let [journey-vector (string-to-vector journey-string)
        reversed-journey-vector (process-parent journey-vector)
        last-page (first reversed-journey-vector)
        subtree (retrieve-subtree last-page)
        suffixes (build-all-suffixes reversed-journey-vector)
        weights (suffix-weights (count suffixes))]
    (iterate-over-suffixes (zipmap suffixes weights) subtree)))
