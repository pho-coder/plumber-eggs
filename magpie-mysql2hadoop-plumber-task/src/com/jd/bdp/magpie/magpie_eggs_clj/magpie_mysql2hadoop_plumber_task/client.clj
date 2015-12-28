(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.client
  (:require [clojure.tools.logging :as log]
            [thrift-clj.core :as thrift]
            [com.jd.bdp.magpie.util.utils :as magpie-utils]
            [clj-zookeeper.zookeeper :as zk]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.utils :as utils])
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap)
  (:import [org.apache.thrift.transport TTransportException]))

(def ^:dynamic *albatross* (atom nil))
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
  [task-id]
  (try
    (let [job-id (utils/get-job-id task-id)]
      (Albatross/getTaskConf @*albatross-client* job-id task-id)
      ; TODO
      BASE-CONF)
    (catch TTransportException e
      (log/error e)
      (reset! *reset-albatross-client* true))))

(defn- prepare-albatross-client
  [albatross-id]
  (let [albatross-node (str ALBATROSSES-PART albatross-id)
        albatross-info (magpie-utils/bytes->map (zk/get-data albatross-node))]
    (reset! *albatross* {:id albatross-id
                         :ip (get albatross-info "ip")
                         :port (get albatross-info "port")})
    (log/info @*albatross*)
    (get-albatross-client (:ip @*albatross*) (:port @*albatross*))))

(defn prepare
  "1、初始化albatross客户端
   2、获取任务配置信息"
  [task-id]
  (let [albatross-id (utils/get-albatross-id task-id)]
    (prepare-albatross-client albatross-id)))

(defn heartbeat
  [job-id task-id status]
  (try
    (Albatross/islandHeartbeat @*albatross-client* job-id task-id status)
    (catch TTransportException e
      (log/error e)
      (reset! *reset-albatross-client* true))))
