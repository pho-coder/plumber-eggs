(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.hdfs
  (:import (org.apache.hadoop.conf Configuration)
           (org.apache.hadoop.fs FileSystem Path)
           (java.net URI)))

(defn ^Configuration configuration
  "Returns the Hadoop configuration."
  []
  (let [config (Configuration.)]
    (.addResource config (Path. "core-site.xml"))
    (.addResource config (Path. "hdfs-site.xml"))
    config))

(defn write
  [buffer str-path]
  (let [str-path (str str-path "." (System/currentTimeMillis))
        conf (configuration)
        hdfs (FileSystem/get (URI/create str-path) conf)
        path (Path. str-path)
        writer (.create hdfs path)]
    (.write writer buffer)
    (.flush writer)
    (.sync writer)
    (.close writer)
    (.close hdfs)))
