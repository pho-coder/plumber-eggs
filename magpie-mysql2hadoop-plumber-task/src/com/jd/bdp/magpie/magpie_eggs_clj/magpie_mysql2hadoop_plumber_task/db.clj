(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db
  (:require [clojure.java.jdbc :as jdbc]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.hdfs :as hdfs]
            [clojure.tools.logging :as log])
  (:import (java.io ByteArrayOutputStream))
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap))

(def spec {:subprotocol "mysql"})
(def ^:dynamic data-buffer (ByteArrayOutputStream. DATA-BUFFER-MAX-SIZE))

(defn query
  [user password db-name sql]
  (let [conf (into spec {:user user :password password :subname (str "//localhost:3306/" db-name)})
        rs (jdbc/query conf sql :as-arrays? true)]
    (map (fn [row] (map #(str %) row)) (rest rs))))

(defn write
  [row-buf str-path]
  (let [buf-len (.size data-buffer)
        row-len (alength row-buf)]
    (if (< (+ buf-len row-len) DATA-BUFFER-MAX-SIZE)
      (.write data-buffer row-buf 0 row-len)
      (try                                                    ;else
        (println "len:" buf-len row-len (+ buf-len row-len))
        (hdfs/write (.toByteArray data-buffer) (str str-path (System/currentTimeMillis)))
        (.reset data-buffer)
        (catch Exception e
          (log/error "IO ERROR:" e))))))
