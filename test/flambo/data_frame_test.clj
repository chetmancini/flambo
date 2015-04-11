(ns flambo.data-frame-test
  (:use midje.sweet)
  (:require [flambo.api :as f]
            [flambo.sql :as sql]
            [flambo.data-frame :as df]
            [flambo.conf :as conf])
  (:import [org.apache.spark.sql SQLContext DataFrame Row Column RDD]))

(facts
  "about data frames"
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "api-test"))]
    (f/with-context c conf
      (let [sql-context (sql/sql-context c)
            test-data ["{\"col1\":4,\"col2\":\"a\"}" "{\"col1\":5,\"col2\":\"a\"}" "{\"col1\":6,\"col2\":\"b\"}"]
            test-df (sql/json-rdd sql-context (f/parallelize c test-data))
            test-data-2 ["{\"col1\":4,\"col2\":\"a\"}" "{\"col1\":5,\"col2\":\"a\"}"]
            test-df-2 (sql/json-rdd sql-context (f/parallelize c test-data-2))]
        (fact "apply returns a column"
          (type (df/apply test-df "col1")) => Column)
        (fact "collect returns the rows as a regular list datat structure"
          (count (df/collect test-df)) => 3)
        (fact "columns returns the columns names"
          (df/columns test-df) => ["col1" "col2"])
        (fact "count returns number of elements in dataframe"
          (df/count test-df) => 3)
        (fact "except returns the difference of two dataframes"
          (df/row->vec (first (df/collect (df/except test-df test-df-2)))) => [6 "b"])
        (fact "explain works"
          (df/explain test-df))
        (fact "filter")
        (fact "group-by")
        (fact "head returns the first row"
          (df/row->vec (df/head test-df)) => [4 "a"])
        (fact "intersect")
        (fact "sort")
        (fact "print schema works"
          (df/print-schema test-df))
        (fact "rdd returns an rdd"
          (type (df/rdd test-df)) => RDD)
        (fact "sample")
        (fact "show"
          (df/show test-df))
        (fact "take")
        (fact "to-df")
        (fact "to-java-rdd")
        (fact "to-json")
        (fact "union all")))))