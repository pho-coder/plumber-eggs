(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.conveyor
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db :as db]
            )
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap))

(def ^:dynamic *reder-status* (atom nil))
(def ^:dynamic *writer-status* (atom nil))

(defn get-reader-status
  []
  @*reder-status*)

(defn task-has-error?
  []
  (if (or (= IO-ERROR @*reder-status*) (= IO-ERROR @*writer-status*))
    true
    false))

(defn reader
  [conf]
  (try
    (let [args ((juxt :user :password :db-name :sql) conf)]
      (doseq [i (apply db/query  args)]
        (.add DATA-CACHE-QUEUE i)
        (Thread/sleep 1000))
      (reset! *reder-status* IO-DONE))
      (catch Exception e
        (log/error "reader error:" e)
        (reset! *reder-status* IO-ERROR))))

(defn writer
  []
  (while true
    (try
      ; 连接数据库
      ; 写入数据
      (if (> (.size DATA-CACHE-QUEUE) 0)
        (let []
          (print "write's size" (.size DATA-CACHE-QUEUE))
          ; TODO WRITE TO HADOOP
          (println " item:" (.poll DATA-CACHE-QUEUE)))
        (do
          (println "write's size" (.size DATA-CACHE-QUEUE))
          (Thread/sleep 100)))
      (catch Exception e
        (log/error "writer error:" e)
        (reset! *writer-status* IO-ERROR)))))

