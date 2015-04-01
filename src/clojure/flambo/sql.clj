;; ## EXPERIMENTAL
;;
;; This code as well as SparkSQL itself are considered experimental.
;;
(ns flambo.sql
  (:require [flambo.api :as f :refer [defsparkfn]])
  (:import [org.apache.spark.sql SQLContext Row]))

;; ## SQLContext
;;
(defn sql-context [spark-context]
  (SQLContext. spark-context))

(defn sql [sql-context query]
  (.sql sql-context query))

(defn parquet-file [sql-context path]
  (.parquetFile sql-context path))

(defn json-file
  "Load a JSON file (one object per line) "
  [sql-context path]
  (.jsonFile sql-context path))

(defn register-data-frame-as-table [sql-context df table-name]
  (.registerDataFrameAsTable sql-context df table-name))

(defn json-rdd
  "Load an RDD of JSON strings (one object per line), inferring the schame, and returning a DataFrame"
  [sql-context json-rdd]
  (.jsonRDD sql-context json-rdd))

(defn cache-table [sql-context table-name]
  (.cacheTable sql-context table-name))

(defn clear-cache [sql-context]
  (.clearCache sql-context))

;; ## DataFrame
;;
(defn register-temp-table [df table-name]
  (.registerTempTable df table-name))

(def print-schema (memfn printSchema))

(def rdd (memfn rdd))

(def sample
  ([data-frame with-replacement fraction]
    (.sample data-frame with-replacement fraction))
  ([data-frame with-replacement fraction seed]
    (.sample data-frame with-replacement fraction seed)))

(def show
  "Displays the top 20 rows of data-frame in a tabular form"
  ([data-frame]
    (.show data-frame))
  ([data-frame n]
    (.show data-frame n)))

(def take
  "Returns the first n rows in data-frame"
  [data-frame n]
  (.take data-frame n))

(def to-df [data-frame column-names]
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
