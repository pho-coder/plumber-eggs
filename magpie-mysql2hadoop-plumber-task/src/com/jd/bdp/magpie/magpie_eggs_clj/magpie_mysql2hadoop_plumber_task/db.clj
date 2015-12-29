(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db
  (:require [clojure.java.jdbc :as jdbc]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.hdfs :as hdfs]
            [clojure.tools.logging :as log])
  (:import (java.io ByteArrayOutputStream)))

(def spec {:subprotocol "mysql"})
(def MAX-SIZE (* 1024 1024 10))
(def ^:dynamic data-buffer (ByteArrayOutputStream. max-size))

(defn query
  [user password db-name sql]
  (let [conf (into spec {:user user :password password :subname (str "//localhost:3306/" db-name)})]
    (jdbc/query conf sql :as-arrays? true)))

(defn write
  [row-buf str-path]
  (while (> MAX-SIZE (.size data-buffer))
    (.write data-buffer row-buf 0 (alength row-buf)))
  (try
    (hdfs/write (.toByteArray data-buffer) str-path)
    (.reset data-buffer)
    (catch Exception e
      (log/error "IO ERROR:" e))))
