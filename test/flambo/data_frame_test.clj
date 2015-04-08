(ns flambo.data-frame-test
  (:use midje.sweet)
  (:require [flambo.api :as f]
            [flambo.sql :as sql]
            [flambo.data-frame :as df]
            [flambo.conf :as conf]))

(facts
  "about data frames"
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "api-test"))]
    (f/with-context c conf
      (let [sql-context (sql/sql-context c)
            test-data (array "{\"first\":4}" "{\"first\":5}" "{\"first\":6}")
            rdd (f/parallelize c test-data)
            test-df (sql/json-rdd sql-context rdd)]
        (fact
          "spark-conf returns a SparkConf object"
          (df/count test-df) => 3)))))