(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db
  (:require [clojure.java.jdbc :as jdbc]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.hdfs :as hdfs]
            [clojure.tools.logging :as log])
  (:import (java.io ByteArrayOutputStream))
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap))

(def spec {:subprotocol "mysql"})
(def ^:dynamic data-buffer (ByteArrayOutputStream. max-size))

(defn query
  [user password db-name sql]
  (let [conf (into spec {:user user :password password :subname (str "//localhost:3306/" db-name)})
        rs (jdbc/query conf sql :as-arrays? true)]
    (doseq [row (rest rs)]
      (map #(str %)) row)))

(defn write
  [row-buf str-path]
  (while (> DATA-BUFFER-MAX-SIZE (.size data-buffer))
    (.write data-buffer row-buf 0 (alength row-buf)))
  (try
    (hdfs/write (.toByteArray data-buffer) str-path)
    (.reset data-buffer)
    (catch Exception e
      (log/error "IO ERROR:" e))))
