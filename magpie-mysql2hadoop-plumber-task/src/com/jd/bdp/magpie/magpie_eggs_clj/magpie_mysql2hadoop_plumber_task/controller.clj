(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.controller
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
  (send-status))

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
    (reset! *task-conf* {:job-id job-id :task-id task-id :uuid uuid :conf conf}))
  (log/info "task conf:" @*task-conf*)
  (reset! *prepared* true))

(defn prepared?
  []
  @*prepared*)

(defn start-task
  []
  (upgrade-and-send-status STATUS-INIT)
  (let [f-reader (future (conveyor/reader @*task-conf*))
        f-writer (future (conveyor/writer))]
    (log/info "reader thread:" @f-reader)
    (log/info "writer thread:" @f-writer)))

