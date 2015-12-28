(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log])
  (:import (org.apache.hadoop.conf Configuration)
           (org.apache.hadoop.fs FileSystem Path)))

(def spec {:subprotocol "mysql"})

(defn query
  [user password db-name sql]
  (let [conf (into spec {:user user :password password :subname (str "//localhost:3306/" db-name)})]
    (jdbc/query conf sql)))

(defn- default-parse-fn
  [row]
  (let [str-row (reduce (fn [acc [k v]]
                          (str acc k "::" v "\t"))
                        ""
                        row)]
    (println (str str-row "\n"))
    (str str-row "\n")))

(defn ^Configuration configuration
  "Returns the Hadoop configuration."
  [] (let [config (Configuration.)]
       (if-let [hadoop_config_dir (System/getenv "HADOOP_CONF_DIR")]
         (do
           (println "HADOOP_CONF_DIR:" hadoop_config_dir)
           (.addResource config (Path. (str hadoop_config_dir "/core-site.xml")))
           (.addResource config (Path. (str hadoop_config_dir "/hdfs-site.xml")))))
       config))

(defn write
  [rows str-path & {:keys [parse-fn]
                    :or {parse-fn default-parse-fn}}]
  (let [conf (configuration)
        hdfs (FileSystem/get conf)
        _ (println (.exists hdfs (Path. str-path)))
        writer (.create hdfs (Path. str-path))]
    (doseq [row rows]
      (try
        (.writeUTF writer (parse-fn row))
        (catch Exception e
          (println e))))
    (.close writer)
    (.close hdfs)))

(defn test-write-fn
  []
  (let [tmp-path "hdfs://localhost:9000/user/xiaochaihu/user.txt"
        rows [{:name "zeng" :age 1 :address "beijing"}
              {:name "zeng" :age 2 :address "beijing2"}
              {:name "zeng" :age 3 :address "beijing3"}
              {:name "zeng" :age 4 :address "beijing4"}
              {:name "zeng" :age 5 :address "beijing5"}
              {:name "zeng" :age 6 :address "beijing6"}]]
    (write rows tmp-path)))
