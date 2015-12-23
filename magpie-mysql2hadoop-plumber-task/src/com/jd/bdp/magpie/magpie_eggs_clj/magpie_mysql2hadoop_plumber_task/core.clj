(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.core
  (:gen-class)
  (:use [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap])
  (:require [clojure.tools.logging :as log]
            [clj-zookeeper.zookeeper :as zk]
            [com.jd.bdp.magpie.magpie-framework-clj.task-executor :as task-executor]
            [com.jd.bdp.magpie.util.utils :as magpie-utils]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.utils :as utils]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.controller :as controller]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.client :as client]))

(def ^:dynamic tmp-start-time (atom nil))

(defn prepare-fn
  [task-id]
  (if (utils/check-task-valid? task-id)
    (log/info "task id valid ok!" task-id)
    (do (log/info "task id NOT valid!" task-id)
        (System/exit 0)))
  (client/prepare task-id)
  (log/info task-id "is preparing!")
  (reset! tmp-start-time (magpie-utils/current-time-millis)))

(defn run-fn [task-id]
  (log/info "run")

  )

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (log/info "Hello, World!")
  (try
    (task-executor/execute run-fn :prepare-fn prepare-fn)
    (catch Throwable e
      (log/error e)
      (log/info "bye"))))
