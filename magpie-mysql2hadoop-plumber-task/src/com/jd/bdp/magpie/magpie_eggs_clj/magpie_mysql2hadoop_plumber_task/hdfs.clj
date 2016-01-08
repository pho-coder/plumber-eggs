(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.hdfs
  (:import (org.apache.hadoop.conf Configuration)
           (org.apache.hadoop.fs FileSystem Path)
           (java.net URI)))

(defn ^Configuration configuration
  "Returns the Hadoop configuration."
  []
  (let [config (Configuration.)]
    (.addResource config (Path. "/usr/local/hadoop/etc/hadoop/core-site.xml"))
    (.addResource config (Path. "/usr/local/hadoop/etc/hadoop/hdfs-site.xml"))
    config))

(defn write
  [buffer str-path]
  (let [conf (configuration)
        hdfs (FileSystem/get (URI/create str-path) conf)
        path (Path. str-path)
        writer (if (.exists hdfs path)
                 (.append hdfs path)
                 (.create hdfs path))]
    (.write writer buffer)
    (.flush writer)
    (.sync writer)
    (.close writer)
    (.close hdfs)))
