(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.controller
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.client :as client]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.utils :as utils])
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap)
  (:import [java.util.concurrent LinkedBlockingQueue]))

(def TASK-DONE "done")
(def TASK-ERROR "error")

(def ^:dynamic *task-status* (atom nil))
;; {:job-id job-id :task-id task-id :uuid :conf conf}
(def ^:dynamic *task-conf* (atom nil))
(def ^:dynamic *prepared* (atom false))
(def ^:dynamic data-cache-queue (LinkedBlockingQueue. QUEEU-LENGTH))
(def ^:dynamic *reder-status* (atom nil))
(def ^:dynamic *writer-status* (atom nil))

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

(defn task-has-error?
  []
  (if (or (= TASK-ERROR @*reder-status*) (= TASK-ERROR @*writer-status*))
    true
    false))

(defn task-done?
  []
  (let [size (.size data-cache-queue)]
    (if (= TASK-DONE @*reder-status*)
      (if (= 0 size) (upgrade-and-send-status STATUS-FINISH) (upgrade-and-send-status STATUS-RUNNING))
      (if (true? (task-has-error?)) (upgrade-and-send-status STATUS-STOP) (upgrade-and-send-status STATUS-RUNNING))))
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
  (reset! *prepared* true))

(defn reader
  []
  (try
    ; 连接数据库
    ; 读取数据
    (doseq [i (range QUEEU-LENGTH)]
      (.add data-cache-queue i)
      (Thread/sleep 1000))
    (reset! *reder-status* TASK-DONE)
    (catch Exception e
      (log/error "reader error:" e)
      (reset! *reder-status* TASK-ERROR))))

(defn writer
  []
  (while true
    (try
      ; 连接数据库
      ; 写入数据
      (if (> (.size data-cache-queue) 0)
        (do
          (print "write's size" (.size data-cache-queue))
          (println " item:" (.poll data-cache-queue)))
        (do
          (println "write's size" (.size data-cache-queue))
          (Thread/sleep 100)))
      (catch Exception e
        (log/error "writer error:" e)
        (reset! *writer-status* TASK-ERROR)))))

(defn start-task
  []
  (upgrade-and-send-status STATUS-INIT)
  (let [f-reader (future (reader))
        f-writer (future (writer))]
    (log/info "reader thread:" @f-reader)
    (log/info "writer thread:" @f-writer)))

