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
(def weights (suffix-weights (count suffixes)))
(def weighted-suffixes (zipmap suffixes weights))
(def family-parents (keys family))

(defn get-matching-journeys [suffix family-parents]
  (filter (fn [parent] (str/starts-with? parent suffix)) family-parents))

(def matching-parents (get-matching-journeys "c|b" family-parents))


(defn abs-to-perc [vector]
  (map #(/ % (reduce + vector)) vector))


(defn get-pages-visits [parent weight tree]
  (let [pages-kw (keys (:child (get tree parent)))
        pages (map name pages-kw)
        visits-str (vals (:child (get tree parent)))
        visits (map #(* % weight) (abs-to-perc (map #(Integer. %) visits-str)))]
    (zipmap pages visits)))
        

(defn get-suffix-children [suffix weight tree]
  (->> [suffix]
       ;; extract matching journeys
       (mapcat #(get-matching-journeys % (keys tree)))
       ;;(fn [s] (filter (fn [parent] (str/starts-with? parent [s])) tree))
       (distinct)
       (sort-by count)
       (reverse)
       ;; retrieve children and visits
       (map #(get-pages-visits % weight tree))
       ;; aggregate resulting hashmap
       (apply merge-with +)))


(defn iterate-over-suffixes [weighted-suffixes tree]
  (let [first-weighted-suffix (first weighted-suffixes)
        first-suffix (first first-weighted-suffix)
        weight (last first-weighted-suffix)]
    (when first-suffix
      (prn first-suffix)
      (prn (get-suffix-children first-suffix weight tree))
      (recur (rest weighted-suffixes) (dissoc tree first-suffix)))))
