(ns flambo.data-frame
  (:require [flambo.api :as f :refer [defsparkfn untuple]])
  (:import [org.apache.spark.sql DataFrame Row Column GroupedData]))


;; ## DataFrame
;;
(defn agg
  "Aggregate the entire dataframe without groups."
  ([^DataFrame df exprs]
   (.agg df exprs)))

(defn apply-column
  "Returns Column based on column name"
  [^DataFrame df ^String column-name]
  (.apply df column-name))

(defn as
  "Returns a new dataframe with an alias"
  [^DataFrame df ^String alias]
  (.as df alias))

(def cache (memfn cache))

(defn col
  "Return the column with this name"
  [^DataFrame df ^String col-name]
  (.col df col-name))

(defn collect
  "Return a list of all the Rows"
  [^DataFrame df]
  (.collectAsList df))

(defn columns
  "Returns all column names as a vector."
  [^DataFrame df]
  (vec (.columns df)))

(defn count
  "Return number of rows in DataFrame or GroupedData Aggregation"
  [df-or-gd]
  (.count df-or-gd))

(defn distinct
  "Return a new dataframe with distinct values"
  [^DataFrame df]
  (.distinct df))

(defn dtypes
  "Return a list of column names and their data types"
  [^DataFrame df]
  (map f/untuple (.dtypes df)))

(defn except
  "Return the difference of this DataFrame and another"
  [^DataFrame df ^DataFrame other]
  (.except df other))

(defn explain
  "Print the physical and optional logical plan to the console for debugging"
  ([col-or-df]
   (.explain col-or-df))
  ([^DataFrame df extended]
   (.explain df extended)))

(defn filter
  "Filters rows using the given SQL expression."
  [^DataFrame df condition-expr]
  (.filter df condition-expr))

(defn first
  "Alias for head"
  ([^DataFrame df]
   (.first df))
  ([^DataFrame df n]
   (.first df n)))

(defn group-by
  "Groups the dataframe by the specified column names"
  [^DataFrame df column]
  (.groupBy df column (into-array String [])))

(defn head
  "Return the first n rows"
  ([^DataFrame df]
   (.head df))
  ([^DataFrame df n]
   (.head df n)))

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
  [^DataFrame df ^String sort-col]
  (.sort df sort-col (into-array String [])))

(defn print-schema
  "Prints the schema to the console in a nice tree format."
  [df]
  (.printSchema df))

(defn rdd
  "Return the content of the DataFrame as an RDD of Rows."
  [df]
  (.rdd df))

(defn sample
  "Returns a new DataFrame by sampling a fraction of rows, optionally using a random seed."
  ([^DataFrame df with-replacement fraction]
   (.sample df with-replacement fraction))
  ([^DataFrame df with-replacement fraction seed]
   (.sample df with-replacement fraction seed)))

(defn show
  "Displays the top 20 rows of data-frame in a tabular form"
  ([^DataFrame df]
   (.show df))
  ([^DataFrame df n]
   (.show df n)))

(defn take
  "Returns the first n rows in data-frame"
  [^DataFrame df n]
  (.take df n))

(defn to-df
  "Returns the DataFrame, optionally with columns renamed."
    ([^DataFrame df]
      (.toDF df))
    ([^DataFrame df cols]
     (.toDF df (into-array String cols))))

(def to-java-rdd (memfn toJavaRDD))

(def to-json (memfn toJSON))

(defn union-all
  "Return a new DataFrame with the union of rows of these DataFrames"
  [^DataFrame df other]
  (.unionAll df other))

;; ##GroupedData
;;
(defn agg
  "Compute aggregates by specifying a map from column name to aggregate methods.
  entries must be 'table_name'=>['avg'|'count'|'max'|'mean'|'min'|'sum']"
  [^GroupedData gd exprs]
  (.agg gd exprs))

(defn avg
  "Compute the mean value for each numeric columns for each group."
  ([^GroupedData gd]
    (.avg gd (into-array String [])))
  ([^GroupedData gd column-names]
    (.avg gd (into-array String column-names))))

;;see Count under DataFrame Section

(defn max
  "Compute the max value for each numeric columns for each group."
  ([^GroupedData gd]
   (.max gd (into-array String [])))
  ([^GroupedData gd column-names]
   (.max gd (into-array String column-names))))

(defn mean
  "Compute the average value for each numeric columns for each group."
  ([^GroupedData gd]
   (.mean gd (into-array String [])))
  ([^GroupedData gd column-names]
   (.mean gd (into-array String column-names))))

(defn min
  "Compute the min value for each numeric column for each group."
  ([^GroupedData gd]
   (.min gd (into-array String [])))
  ([^GroupedData gd column-names]
   (.min gd (into-array String column-names))))

(defn sum
  "Compute the sum for each numeric columns for each group."
  ([^GroupedData gd]
   (.sum gd (into-array String [])))
  ([^GroupedData gd column-names]
   (.sum gd (into-array String column-names))))

;; ## Row
;;
(defsparkfn row->vec [^Row row]
  (let [n (.length row)]
    (loop [i 0 v (transient [])]
      (if (< i n)
        (recur (inc i) (conj! v (.get row i)))
        (persistent! v)))))

;; ## Column
;;
(defn is-not-null?
  "SQL is-not-null. Returns a Column"
  [^Column col]
  (.isNotNull col))

(defn is-null?
  "SQL is-null. Returns a Column"
  [^Column col]
  (.isNull col))

(defn starts-with?
  "SQL starts with. Returns another Column. Second param can be string or a Column"
  [^Column col other]
  (.startsWith col other))