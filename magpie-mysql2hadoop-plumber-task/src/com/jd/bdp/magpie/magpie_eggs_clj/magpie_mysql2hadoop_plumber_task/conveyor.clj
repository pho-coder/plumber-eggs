(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.conveyor
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db :as db]
            )
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap))

(def ^:dynamic *reader-status* (atom nil))
(def ^:dynamic *writer-status* (atom nil))

(def ^:dynamic *done-thread-num* (atom 0))
(def ^:dynamic *read-thread-num* (atom 0))

(defn get-reader-status
  []
  @*reader-status*)

(defn task-has-error?
  []
  (if (or (= IO-ERROR @*reader-status*) (= IO-ERROR @*writer-status*))
    true
    false))

(defn all-read-thread-done?
  []
  (println @*done-thread-num* @*read-thread-num*)
  (if (= @*done-thread-num* @*read-thread-num*)
    true
    false))

(defn start-query-thread
  [user password db-name sql host]
  (let [athread (future
                  (try
                    (doseq [row (db/query user password db-name sql host)]
                      (println row)
                      (while (>= (.size DATA-CACHE-QUEUE) QUEEU-LENGTH)
                        (log/info "DATA-CACHE-QUEUE is full.")
                        (Thread/sleep 1000))
                      (.add DATA-CACHE-QUEUE row))
                    (swap! *done-thread-num* inc)
                    (catch Exception e
                      (log/error "reader error:" e)
                      (reset! *reader-status* IO-ERROR))))]
    @athread))

(defn reader
  [taks-conf]
  (let [conf (:conf taks-conf)
        [user password db-name sqls host] ((juxt :user :password :db-name :sqls :host) conf)]
    ; 需要启动的线程总数
    (reset! *read-thread-num* (count sqls))
    (doseq [sql sqls]
      (start-query-thread user password db-name sql host))))

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
            (println "queue's size =" (.size DATA-CACHE-QUEUE))
            (let [row (.poll DATA-CACHE-QUEUE)
                  queue-size (.size DATA-CACHE-QUEUE)
                  all-done (and (= queue-size 0) (all-read-thread-done?))
                  ; TODO 转化为字符串，去除特殊字符
                  row-str (str (clojure.string/join "\t" row) "\n")
                  row-buf (.getBytes row-str)]
              ; TODO WRITE TO HADOOP
              (db/write row-buf (:target conf) all-done)
              (if (true? all-done)
                (do
                  (reset! *reader-status* IO-DONE)
                  (reset! *writer-status* IO-DONE)))))
          (do
            (println "queue's size =" (.size DATA-CACHE-QUEUE))
            (println "all read thread done? " (all-read-thread-done?))
            (Thread/sleep 1000))))
      (catch Exception e
        (log/error "writer error:" e)
        (reset! *writer-status* IO-ERROR)))))
