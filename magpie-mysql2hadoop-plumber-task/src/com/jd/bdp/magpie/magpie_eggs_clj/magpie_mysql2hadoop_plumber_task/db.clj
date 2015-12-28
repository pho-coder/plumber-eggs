(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db
  (:require [clojure.java.jdbc :as jdbc])
  (:import (org.apache.hadoop.conf Configuration)
           (org.apache.hadoop.fs FileSystem Path)
           (java.net URI)))

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
    (str str-row "\n")))

(defn- write-data
  [writer str-row]
  (let [byte-row (.getBytes str-row)
        row-len (.length byte-row)]
    (.write writer byte-row 0 row-len)))

(defn write-append
  [rows str-path & {:keys [parse-fn]
                    :or {parse-fn default-parse-fn}}]
  (let [conf (Configuration.)
        _ (.setBoolean conf "dfs.support.append" true)
        hdfs (FileSystem/get (URI/create str-path) conf)
        writer (.append hdfs (Path. str-path))]
    (println "ssss")
    (doseq [row rows]
      (try
        (write-data writer (parse-fn row))
        (catch Exception e
          (println e))))
    (.close writer)
    (.close hdfs)))

(defn write
  [rows str-path & {:keys [parse-fn]
                    :or {parse-fn default-parse-fn}}]
  (let [conf (Configuration.)
        hdfs (FileSystem/get conf)
        _ (println (Path. str-path))
        _ (println (.exists hdfs (Path. str-path)))
        _ (println "deleted?" (.deleteOnExit hdfs (Path. str-path)))
        writer (.create hdfs (Path. str-path))]
    (doseq [row rows]
      (try
        (println row)
        (.writeUTF writer (parse-fn row))
        (catch Exception e
          (println e))))
    (.close writer)
    (.close hdfs)))

(defn test-write-fn
  []
  (let [                                                    ;tmp-path "/user/xiaochaihu/input/user.txt"
        tmp-path "user.txt"
        rows [{:name "zeng" :age 1 :address "beijing"}
              {:name "zeng" :age 2 :address "beijing2"}
              {:name "zeng" :age 3 :address "beijing3"}
              {:name "zeng" :age 4 :address "beijing4"}
              {:name "zeng" :age 5 :address "beijing5"}
              {:name "zeng" :age 6 :address "beijing6"}]]
    (write rows tmp-path)))
