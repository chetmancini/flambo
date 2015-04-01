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
