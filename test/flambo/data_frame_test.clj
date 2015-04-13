(ns flambo.data-frame-test
  (:use midje.sweet)
  (:require [flambo.api :as f]
            [flambo.sql :as sql]
            [flambo.data-frame :as df]
            [flambo.conf :as conf])
  (:import [org.apache.spark.sql SQLContext DataFrame Row Column]
           [org.apache.spark.rdd MapPartitionsRDD]
           [org.apache.spark.api.java JavaRDD]))

(facts
  "about data frames"
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "api-test"))]
    (f/with-context c conf
      (let [sql-context (sql/sql-context c)
            test-data ["{\"col1\":4,\"col2\":\"a\"}" "{\"col1\":6,\"col2\":\"a\"}" "{\"col1\":5,\"col2\":\"b\"}"]
            test-df (sql/json-rdd sql-context (f/parallelize c test-data))
            test-data-2 ["{\"col1\":4,\"col2\":\"a\"}" "{\"col1\":4,\"col2\":\"a\"}" "{\"col1\":6,\"col2\":\"a\"}"]
            test-df-2 (sql/json-rdd sql-context (f/parallelize c test-data-2))]
        (fact "aggregate the dataframe")
        (fact "apply returns a column"
          (type (df/apply-column test-df "col1")) => Column)
        (fact "as returns dataframe with an alias"
          (type (df/as test-df "new-name")) => DataFrame)
        (fact "collect returns the rows as a regular list datat structure"
          (count (df/collect test-df)) => 3)
        (fact "columns returns the columns names"
          (df/columns test-df) => ["col1" "col2"])
        (fact "count returns number of elements in dataframe"
          (df/count test-df) => 3)
        (fact "distinct returns only distinct rows"
          (df/count (df/distinct test-df-2)) => 2)
        (fact "dtypes returns the datatypes"
          (df/dtypes test-df) => [["col1" "LongType"] ["col2" "StringType"]])
        (fact "except returns the difference of two dataframes"
          (df/row->vec (first (df/collect (df/except test-df test-df-2)))) => [5 "b"])
        (fact "explain works"
          (df/explain test-df))
        (fact "filter"
          (df/count (df/filter test-df "col2 = \"a\"")) => 2)
        (fact "group-by")
        (fact "head returns the first row"
          (df/row->vec (df/head test-df)) => [4 "a"])
        (fact "intersect"
          (df/count (df/intersect test-df test-df-2)) => 2)
        (fact "Cartesian join"
          (df/count (df/join test-df test-df-2)) => 9)
        (fact "register as temp table works"
          (df/register-temp-table test-df "test-table-name"))
        (fact "sort the dataframe"
          (df/row->vec (last (df/collect (df/sort test-df "col1")))) => [6 "a"])
        (fact "print schema works"
          (df/print-schema test-df))
        (fact "rdd returns an rdd"
          (type (df/rdd test-df)) => MapPartitionsRDD)
        (fact "sample"
          (df/sample test-df true 1.0))
        (fact "show"
          (df/show test-df))
        (fact "take returns a Row[] of the first n rows"
          (count (df/take test-df 1)) => 1)
        (fact "to-df"
          (df/columns (df/to-df test-df ["cola" "colb"])) => ["cola" "colb"])
        (fact "to-java-rdd"
          (type (df/to-java-rdd test-df)) => JavaRDD)
        (fact "to-json"
          (f/first (df/to-json test-df)) => "{\"col1\":4,\"col2\":\"a\"}")
        (fact "union all"
          (df/count (df/union-all test-df test-df-2)) => 6)))))