(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.controller)

(def ^:dynamic *task-status* (atom nil))

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
