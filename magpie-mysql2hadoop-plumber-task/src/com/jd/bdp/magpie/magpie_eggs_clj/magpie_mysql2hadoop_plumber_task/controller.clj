(ns ^{:author "xiaochaihu"
      :doc "主要职责是：启动前准备、启动数据抽取工作线程、上发抽取任务的状态
            1、启动前准备：连接albatross服务，获取任务配置信息
            2、启动抽取、写入的线程
            3、定期检查任务完成状态，向albatross服务上发任务状态"}
  com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.controller
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.client :as client]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.conveyor :as conveyor]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.utils :as utils])
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap))

(def ^:dynamic *task-status* (atom nil))
;; {:job-id job-id :task-id task-id :uuid :conf conf}
(def ^:dynamic *task-conf* (atom nil))
(def ^:dynamic *prepared* (atom false))

(defn get-task-status
  []
  @*task-status*)

(defn- send-status
  "向albatross服务发送心跳，报告当前状态"
  []
  (client/heartbeat (:job-id @*task-conf*) (:task-id @*task-conf*) (get-task-status)))

(defn upgrade-and-send-status
  "任务的状态不能倒退
  init -> running -> (finish|stop)"
  [status]
  (reset! *task-status* status)
  (future (send-status)))

(defn task-done?
  []
  (let [size (.size DATA-CACHE-QUEUE)]
    (if (= IO-DONE (conveyor/get-reader-status))
      (if (= 0 size) (upgrade-and-send-status STATUS-FINISH) (upgrade-and-send-status STATUS-RUNNING))
      (if (true? (conveyor/task-has-error?)) (upgrade-and-send-status STATUS-STOP) (upgrade-and-send-status STATUS-RUNNING))))
  (condp = @*task-status*
    STATUS-FINISH true
    false))

(defn prepare
  [task-id]
  (client/prepare task-id)
  (let [job-id (utils/get-job-id task-id)
        uuid (utils/get-task-uuid task-id)
        conf (client/get-conf task-id)]
    (log/info "type of conf" (type conf))
    (reset! *task-conf* {:job-id job-id :task-id task-id :uuid uuid :conf conf}))
  (log/info "task conf:" @*task-conf*)
  (reset! *prepared* true))

(defn prepared?
  []
  @*prepared*)

(defn start-task
  [& {:keys [path]}]
  (upgrade-and-send-status STATUS-INIT)
  ;(reset! *task-conf* {:job-id "job-id" :task-id "task-id" :uuid "uuid" :conf (assoc BASE-CONF :target path)})
  (println @*task-conf*)
  (let [f-reader (future (conveyor/reader @*task-conf*))
        f-writer (future (conveyor/writer @*task-conf*))]
    (log/info "reader thread:" @f-reader)
    (log/info "writer thread:" @f-writer)))

