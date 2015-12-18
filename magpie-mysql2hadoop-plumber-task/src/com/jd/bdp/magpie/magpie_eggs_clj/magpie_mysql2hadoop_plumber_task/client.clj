(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.client
  (:require [clojure.tools.logging :as log]
            [thrift-clj.core :as thrift]
            [com.jd.bdp.magpie.util.utils :as magpie-utils]
            [clj-zookeeper.zookeeper :as zk])
  (:import [org.apache.thrift.transport TTransportException]))

(def ^:dynamic *albatross-client* (atom nil))
(def ^:dynamic *reset-albatross-client* (atom nil))

(thrift/import
 (:clients com.jd.bdp.magpie.albatross.generated.Albatross))

(defn get-albatross-client
  [ip port]
  (log/info "get albatross client!")
  (try
    (reset! *albatross-client* (thrift/connect! Albatross [ip port]))
    (catch Throwable e
      (log/error "get-albatross-client" e)
      (reset! *reset-albatross-client* true))))

(defn get-conf
  [job-id task-id]
  (try
    (let [conf (Albatross/getTaskConf @*albatross-client* job-id task-id)]
      conf)
    (catch TTransportException e
      (log/error e)
      (reset! *reset-albatross-client* true))))

(defn- prepare-albatross-client
  [albatross-id]
  (let [albatrosses-path "/albatross/albatrosses/"
        albatross-node (str albatrosses-path albatross-id)
        albatross-info (magpie-utils/bytes->map (zk/get-data albatross-node))
        albatross-ip (get albatross-info "ip")
        albatross-port (get albatross-info "port")]
    (reset! albatross {:id albatross-id
                       :ip albatross-ip
                       :port albatross-port})
    (log/info @albatross)
    (get-albatross-client (:ip @albatross) (:port @albatross))))

(defn prepare
  "1、初始化albatross客户端
   2、获取任务配置信息"
  [task-id]
  (let [[_ albatross-id job-id _] (clojure.string/split task-id SEPARATOR)]
    (prepare-albatross-client albatross-id)
    (get-conf job-id task-id)))


(defn heartbeat
  [job-id task-id status]
  (try
    (Albatross/islandHeartbeat @*albatross-client* job-id task-id status)
    (catch TTransportException e
      (log/error e)
      (reset! *reset-albatross-client* true))))
