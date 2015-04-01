(ns flambo.sql
  (:require [flambo.api :as f :refer [defsparkfn untuple]])
  (:import [org.apache.spark.sql SQLContext DataFrame Row]))


;; ## DataFrame
;;
(defn apply
  "Returns Column based on column name"
  [df column-name]
  (.apply df column-name))

(def cache (memfn cache))

(defn collect
  "Return a list of all the Rows"
  [df]
  (list (.collectAsList df)))

(def columns
  "Returns all column names as a vector."
  [df]
  (vec (.columns df)))

(def count (memfn count))

(defn dtypes
  "Return a list of column names and their data types"
  [df]
  (map f/untuple (.dtypes df))

(defn except
  "Return the difference of this DataFrame and another"
  [df other]
  (.except df other))

(defn register-temp-table
  "Registers this DataFrame as a temp table with the given name"
  [df table-name]
  (.registerTempTable df table-name))

(def print-schema
  "Prints the schema to the console in a nice tree format."
  [df]
  (.printSchema df))

(def rdd
  "Return the content of the DataFrame as an RDD of Rows."
  [df]
  (.rdd df))

(defn sample
  "Returns a new DataFrame by sampling a fraction of rows, optionally using a random seed."
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

(defn to-df
  "Returns the DataFrame, optionally with columns renamed."
  ([df]
    (.toDF df))
  ([df column-names]
    (.toDF df column-names)))

(def to-java-rdd (memfn toJavaRDD))

(def to-json (memfn toJSON))

(defn union-all
  "Return a new DataFrame with the union of rows of these DataFrames"
  [df other]
  (.unionAll df other))

;; ## Row
;;
(defsparkfn row->vec [^Row row]
  (let [n (.length row)]
    (loop [i 0 v (transient [])]
      (if (< i n)
        (recur (inc i) (conj! v (.get row i)))
        (persistent! v)))))
