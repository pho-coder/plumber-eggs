(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log])
  (:import (org.apache.hadoop.conf Configuration)
           (org.apache.hadoop.fs FileSystem Path)))

(def ^:dynamic *hdfs-connected* (atom false))
(def ^:dynamic *hdfs* (atom nil))
(def ^:dynamic *writer* (atom nil))

(def spec {:subprotocol "mysql"})

(defn query
  [user password db-name sql]
  (let [conf (into spec {:user user :password password :subname (str "//localhost:3306/" db-name)})]
    (jdbc/query conf sql)))

(defn ^Configuration configuration
  "Returns the Hadoop configuration."
  []
  (let [config (Configuration.)]
    (if-let [hadoop_config_dir (System/getenv "HADOOP_CONF_DIR")]
      (do
        (log/info "HADOOP_CONF_DIR:" hadoop_config_dir)
        (.addResource config (Path. (str hadoop_config_dir "/core-site.xml")))
        (.addResource config (Path. (str hadoop_config_dir "/hdfs-site.xml")))))
    config))

(defn- connect-hdfs!
  [str-path]
  (let [conf (configuration)
        hdfs (FileSystem/get conf)
        writer (.create hdfs (Path. str-path))]
    (reset! *hdfs* hdfs)
    (reset! *writer* writer)))

(defn- close
  []
  (.close @*writer*)
  (.close @*hdfs*))

(defn write
  [row str-path]
  (if-not @*writer*
    (connect-hdfs! str-path))
  (try
    (.write @*writer* row 0 (.length row))
    (catch Exception e
      (log/error "HDFS IO error:" e))))

(defn flush
  []
  (.flush @*writer*))
