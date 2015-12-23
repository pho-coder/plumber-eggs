(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.controller
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.client :as client]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.utils :as utils]))

(def ^:dynamic *task-status* (atom nil))
;; {:job-id job-id :task-id task-id :uuid :conf conf}
(def ^:dynamic *task-conf* (atom nil))
(def ^:dynamic *prepareed* (atom false))

(defn upgrade-task-status
  "任务的状态不能倒退
  init -> runing -> (finish|stop)"
  [status]
  (reset! *task-status* status))

(defn start-task
  []
  (upgrade-task-status "init")
  (doto
    (Thread. #((log/info "start task, config:" (:conf @*task-conf*))
               (try
                 (Thread/sleep 10000)
                 (upgrade-task-status "running")
                 ; TODO 执行任务 sql -> reader -> data -> writter
                 (Thread/sleep 120000)
                 (upgrade-task-status "done")
                 (catch Exception e
                   (log/error "error" e)
                   (upgrade-task-status "error")))
               (log/info "task final status:" *task-status*)))
    (.setDaemon true)
    (.start)))

(defn get-task-status
  []
  @*task-status*)

(defn send-heartbeat
  "向albatross服务发送心跳，报告当前状态"
  []
  (client/heartbeat (:job-id @*task-conf*) (:task-id @*task-conf*) (get-task-status)))

(defn task-done?
  []
  (condp = @*task-status*
    "done" true
    false))

(defn prepare
  [task-id]
  (client/prepare task-id)
  (let [job-id (utils/get-job-id task-id)
        uuid (utils/get-task-uuid task-id)
        conf (client/get-conf task-id)]
    (reset! *task-conf* {:job-id job-id :task-id task-id :uuid uuid :conf conf}))
  (reset! *prepareed* true))
