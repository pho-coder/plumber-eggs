(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db
  (:require [clojure.java.jdbc :as jdbc]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.hdfs :as hdfs]
            [clojure.tools.logging :as log])
  (:import (java.io ByteArrayOutputStream))
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap))

(def spec {:subprotocol "mysql"})
(def ^:dynamic data-buffer (ByteArrayOutputStream. DATA-BUFFER-MAX-SIZE))

(defn query
  [user password db-name sql host]
  (let [conf (into spec {:user user :password password :subname (str "//" host ":3306/" db-name)})
        rs (jdbc/query conf sql :as-arrays? true)]
    (map (fn [row] (map #(str %) row)) (rest rs))))

(defn- write-cache-to-db
  [str-path]
  ; 根据不同的 target-type 使用不同的write方法，写入对应的数据库或文件
  (hdfs/write (.toByteArray data-buffer) str-path)
  (.reset data-buffer))

(defn- write-row-buf-to-cache
  [row-buf row-len]
  (.write data-buffer row-buf 0 row-len))

(defn write
  [row-buf str-path all-done & {:keys [target-type]
                                     :or {target-type nil}}]
  (let [buf-len (.size data-buffer)
        row-len (alength row-buf)
        queue-will-overflow (>= (+ buf-len row-len) DATA-BUFFER-MAX-SIZE)]

    (if (true? queue-will-overflow)
      (write-cache-to-db str-path))
    ; 把当前的 row-buf
    (write-row-buf-to-cache row-buf row-len)
    ; 如果
    (if (true? all-done)
      (do (println "所有读写结束，写入剩下的数据。")
          (write-cache-to-db str-path)))))
