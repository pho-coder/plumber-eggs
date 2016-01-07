(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db
  (:require [clojure.java.jdbc :as jdbc]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.hdfs :as hdfs]
            [clojure.tools.logging :as log])
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap))

; 查询方法
(defmulti query (fn [_ db-type] db-type))

(defmethod query "mysql"
  [task-conf sql db-type]
  (log/info "type of source db:" db-type)
  (let [spec {:subprotocol "mysql"}
        [user password db-name host] ((juxt :user :password :db-name :host) task-conf)
        conf (into spec {:user user :password password :subname (str "//" host ":3306/" db-name)})
        rs (jdbc/query conf sql :as-arrays? true)]
    (map (fn [row] (map #(str %) row)) (rest rs))))


; 写入方法
(defmulti write (fn [_ _ db-type] db-type))

(defmethod write "hdfs"
  [task-conf buffer db-type]
  (let [str-path (:target task-conf)]
    (log/info "type of target db:" db-type)
    (hdfs/write buffer str-path)))
