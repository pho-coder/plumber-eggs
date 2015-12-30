(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.conveyor
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db :as db]
            )
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap))

(def ^:dynamic *reader-status* (atom nil))
(def ^:dynamic *writer-status* (atom nil))

(def ^:dynamic *all-read-thread-status* (atom '()))
(def ^:dynamic *read-thread-num* (atom 0))

(defn get-reader-status
  []
  @*reader-status*)

(defn task-has-error?
  []
  (if (or (= IO-ERROR @*reader-status*) (= IO-ERROR @*writer-status*))
    true
    false))

(defn start-query-thread
  ; 现在为单线程
  [user password db-name sql]
  (try
    (doseq [row (db/query user password db-name sql)]
      (.add DATA-CACHE-QUEUE row))
    (reset! *reader-status* IO-DONE)
    (catch Exception e
      (log/error "reader error:" e)
      (reset! *reader-status* IO-ERROR))))

(defn start-query-thread
  ; 现在为单线程
  [user password db-name sql]
  (let [athread (future
                  (try
                    (doseq [row (db/query user password db-name sql)]
                      (.add DATA-CACHE-QUEUE row)
                      ; TODO
                      (Thread/sleep 1000))
                    (swap! *all-read-thread-status* conj IO-DONE)
                    (catch Exception e
                      (log/error "reader error:" e)
                      (reset! *reader-status* IO-ERROR))))]
    (swap! *read-thread-num* inc 1)
    @athread))

(defn reader
  [taks-conf]
  (let [conf (:conf taks-conf)
        [user password db-name sqls] ((juxt :user :password :db-name :sqls) conf)]
    (println "reader args:" user password db-name sqls)
    (doseq [sql sqls]
      (start-query-thread user password db-name sql))))

(defn writer
  [task-conf]
  (let [conf (:conf task-conf)
        uuid (:uuid task-conf)
        job-id (:job-id task-conf)
        task-id (:task-id task-conf)]
    (try
      (while true
        (if (> (.size DATA-CACHE-QUEUE) 0)
          (do
            (println "size of queue:" (.size DATA-CACHE-QUEUE))
            (let [row (.poll DATA-CACHE-QUEUE)
                  _ (println row)
                  ; TODO 转化为字符串，去除特殊字符
                  row-str (str (clojure.string/join "\t" row) "\n")
                  row-buf (.getBytes row-str)]
              ; TODO WRITE TO HADOOP
              (db/write row-buf (:target conf))))
          (do
            (println "write's size" (.size DATA-CACHE-QUEUE))
            (Thread/sleep 1000))))
      (catch Exception e
        (log/error "writer error:" e)
        (reset! *writer-status* IO-ERROR)))))
