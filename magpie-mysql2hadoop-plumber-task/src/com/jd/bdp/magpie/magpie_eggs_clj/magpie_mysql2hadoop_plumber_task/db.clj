(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db
  (:require [clojure.java.jdbc :as jdbc]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.hdfs :as hdfs]
            [clojure.tools.logging :as log]))

(def spec {:subprotocol "mysql"})

(defn query
  [user password db-name sql]
  (let [conf (into spec {:user user :password password :subname (str "//localhost:3306/" db-name)})]
    (jdbc/query conf sql :as-arrays? true)))

(defn write
  [row str-path]
  (hdfs/write row str-path))
