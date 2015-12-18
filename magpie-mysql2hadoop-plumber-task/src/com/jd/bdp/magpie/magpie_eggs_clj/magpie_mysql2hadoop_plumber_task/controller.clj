(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.controller)

(def ^:dynamic *task-status* (atom nil))
;; {:job-id job-id :task-id task-id :uuid :conf conf}
(def ^:dynamic task (atom nil))

(defn send-heartbeat
  "向albatross服务发送心跳，报告当前状态"
  []
  )

(defn task-done?
  [])

(defn get-task-status
  []
  )

(defn upgrade-task-status
  "任务的状态不能倒退
  init -> runing -> (finish|stop)"
  [status]
  (reset! *task-status* status))
