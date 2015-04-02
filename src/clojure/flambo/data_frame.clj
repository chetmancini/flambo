(ns flambo.data-frame
  (:require [flambo.api :as f :refer [defsparkfn untuple]])
  (:import [org.apache.spark.sql SQLContext DataFrame Row Column]))


;; ## DataFrame
;;
(defn agg
  "Aggregate the entire dataframe without groups."
  ([^DataFrame df exprs]
   (.agg df exprs))
  ([df ^Column expr exprs]))

(defn apply
  "Returns Column based on column name"
  [^DataFrame df column-name]
  (.apply df column-name))

(defn as
  "Returns a new dataframe with an alias"
  [df alias]
  (.as df alias))

(def cache (memfn cache))

(defn col
  "Return the column with this name"
  [df col-name]
  (.col df col-name))

(defn collect
  "Return a list of all the Rows"
  [df]
  (list (.collectAsList df)))

(defn columns
  "Returns all column names as a vector."
  [^DataFrame df]
  (vec (.columns df)))

(def count
  "Return number of rows in DataFrame"
  [^DataFrame df]
  (.count df))

(defn dtypes
  "Return a list of column names and their data types"
  [^DataFrame df]
  (map f/untuple (list (.dtypes df)))

(defn except
  "Return the difference of this DataFrame and another"
  [df other]
  (.except df other))

(defn explain
  "Print the physical and optional logical plan to the console for debugging"
  ([^DataFrame df]
   (.explain df))
  ([^DataFrame df extended]
   (.explain df extended)))

(defn filter
  "Filters rows using the given SQL expression."
  [^DataFrame df condition-expr]
  (.filter df condition-expr))

(defn group-by
  "Groups the dataframe by the specified column names"
  [^DataFrame df ^Column column & columns]
  (.groupBy df column columns))

(defn head
  "Return the first n rows"
  ([df]
   (.head df))
  ([df n]
   (.head df n))))


(defn intersect
  "Return a dataframe with rows that only belong in this and the other dataframe."
  [df other]
  (.intersect df other))

(defn join
  "Cartesian join with another df"
  ([^DataFrame df ^DataFrame right]
   (.join df right))
  ([^DataFrame df ^DataFrame right ^Column join-exprs]
   (.join df right join-exprs))
  ([^DataFrame df ^DataFrame right ^Column join-exprs join-type]
   {:pre [(contains? #{:inner :outer :left_outer :right_outer :semijoin} join-type)]}
   (.join df right join-exprs (str join-type))))

(defn register-temp-table
  "Registers this DataFrame as a temp table with the given name"
  [df table-name]
  (.registerTempTable df table-name))

(defn sort
  "Returns a new DataFrame sorted by the specified column."
  [^DataFrame df sort-col]
  (.sort df sort-col))

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
  ([^DataFrame df with-replacement fraction]
   (.sample df with-replacement fraction))
  ([^DataFrame df with-replacement fraction seed]
   (.sample data-frame with-replacement fraction seed)))

(defn show
  "Displays the top 20 rows of data-frame in a tabular form"
  ([^DataFrame df]
   (.show data-frame))
  ([^DataFrame df n]
   (.show data-frame n)))

(defn take
  "Returns the first n rows in data-frame"
  [^DataFrame df n]
  (.take df n))

(defn to-df
  "Returns the DataFrame, optionally with columns renamed."
  ([^DataFrame df]
    (.toDF df))
  ([^DataFrame df column-names]
    (.toDF df column-names)))

(def to-java-rdd (memfn toJavaRDD))

(def to-json (memfn toJSON))

(defn union-all
  "Return a new DataFrame with the union of rows of these DataFrames"
  [^DataFrame df other]
  (.unionAll df other))

;; ## Row
;;
(defsparkfn row->vec [^Row row]
  (let [n (.length row)]
    (loop [i 0 v (transient [])]
      (if (< i n)
        (recur (inc i) (conj! v (.get row i)))
        (persistent! v)))))
