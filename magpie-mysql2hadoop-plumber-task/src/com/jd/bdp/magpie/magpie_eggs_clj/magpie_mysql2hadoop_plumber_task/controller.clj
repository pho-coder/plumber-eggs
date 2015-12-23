(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.controller
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.client :as client]))

(def ^:dynamic *task-status* (atom nil))
;; {:job-id job-id :task-id task-id :uuid :conf conf}
(def ^:dynamic *task-conf* (atom nil))

(defn upgrade-task-status
  "任务的状态不能倒退
  init -> runing -> (finish|stop)"
  [status]
  (reset! *task-status* status))

(defn start-task
  []
  (doto
    (Thread. #((log/info "start task, config:" @*task-conf*)
               (upgrade-task-status "running")
               (try
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
  (let [conf (client/get-conf task-id)]
    (swap! *task-conf* into {})))
