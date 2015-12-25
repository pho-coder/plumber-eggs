(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.core
  (:gen-class)
  (:use [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap])
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-framework-clj.task-executor :as task-executor]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.utils :as utils]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.controller :as controller]))

(defn prepare-fn
  [task-id]
  (if (utils/check-task-valid? task-id)
    (log/info "task id valid ok!" task-id)
    (do (log/info "task id NOT valid!" task-id)
        (System/exit 0)))
  (controller/prepare task-id)
  (controller/start-task))

(defn run-fn [task-id]
  (log/info "task id=" task-id)
  (while (not (controller/prepared?))
    (log/info "task is preparing..."))
  (log/info "task is done=" (controller/task-done?))
  (log/info "task status=" (controller/get-task-status))
  (Thread/sleep 10000))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (log/info "Hello, World!")
  (try
    (task-executor/execute run-fn :prepare-fn prepare-fn)
    (catch Throwable e
      (log/error e)
      (log/info "bye"))))
