(ns ^{:author "xiaochaihu"
      :doc "主要职责是：根据不同的数据来源和目标数据源，编写对应的数据处理方法
            1、根据数据来源定义数据抽取过程，抽取前准备，调用对应的抽取方法
            2、根据目标数据输出地作相应准备，连接数据库，调用对应的写入方法"}
  com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; 写入方法
(defmulti write (fn [_ _ db-type] db-type))

(defmethod write "hdfs"
  [task-conf buffer db-type]
  (let [str-path (:target task-conf)]
    (log/info "type of target db:" db-type)
    (hdfs/write buffer str-path)))
