(ns flambo.sql
  (:require [flambo.api :as f :refer [defsparkfn]])
  (:import [org.apache.spark.sql SQLContext DataFrame Row]))


;; ## DataFrame
;;
(defn apply
  "Returns Column based on column name"
  [df column-name]
  (.apply df column-name))

(def cache (memfn cache))

(def columns [df]
  (vec (.columns df)))

(def count (memfn count))

(defn register-temp-table [df table-name]
  (.registerTempTable df table-name))

(def print-schema (memfn printSchema))

(def rdd (memfn rdd))

(defn sample
  ([data-frame with-replacement fraction]
   (.sample data-frame with-replacement fraction))
  ([data-frame with-replacement fraction seed]
   (.sample data-frame with-replacement fraction seed)))

(defn show
  "Displays the top 20 rows of data-frame in a tabular form"
  ([data-frame]
   (.show data-frame))
  ([data-frame n]
   (.show data-frame n)))

(defn take
  "Returns the first n rows in data-frame"
  [data-frame n]
  (.take data-frame n))

(defn to-df [data-frame column-names]
  (.toDF data-frame column-names))

(def to-java-rdd (memfn toJavaRDD))

(def to-json (memfn toJSON))

;; ## Row
;;
(defsparkfn row->vec [^Row row]
            (let [n (.length row)]
              (loop [i 0 v (transient [])]
                (if (< i n)
                  (recur (inc i) (conj! v (.get row i)))
                  (persistent! v)))))
