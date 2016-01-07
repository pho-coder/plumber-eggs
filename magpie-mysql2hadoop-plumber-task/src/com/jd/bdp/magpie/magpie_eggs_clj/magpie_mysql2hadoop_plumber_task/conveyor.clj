(ns ^{:author "xiaochaihu"
      :doc "主要职责是：读取数据、前期数据格式规范、缓存数据、写入数据库
            1、读取数据库中每行数据，转换为文本格式
            2、把数据写入队列中
            3、把数据从队列中读取出来，去除特殊字符，把数据写入缓冲区中
            4、当缓冲区数据量满足写入数据库条件时，执行写入操作"}
  com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.conveyor
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.db :as db])
  (:use com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap)
  (:import (java.io ByteArrayOutputStream)))

(def ^:dynamic *reader-status* (atom nil))
(def ^:dynamic *writer-status* (atom nil))

(def ^:dynamic *done-thread-num* (atom 0))
(def ^:dynamic *read-thread-num* (atom 0))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:dynamic data-buffer (ByteArrayOutputStream. DATA-BUFFER-MAX-SIZE))

(defn- write-cache-to-db
  [task-conf]
  ; 根据不同的 target 使用不同的write方法，写入对应的数据库或文件
  (db/write task-conf data-buffer (:target task-conf))
  (.reset data-buffer))

(defn- write-row-buf-to-cache
  [row-buf row-len]
  (.write data-buffer row-buf 0 row-len))

(defn write
  "conf 写入数据时所需的参数
   row-buf 数据
   all-done 是否全部结束（最后一次写入）"
  [conf row-buf all-done]
  (let [buf-len (.size data-buffer)
        row-len (alength row-buf)
        queue-will-overflow (>= (+ buf-len row-len) DATA-BUFFER-MAX-SIZE)]
    ; 1、检查队列是否将要溢出，如果将要溢出，则先把队列中的内容先写入数据库
    (if (true? queue-will-overflow)
      (write-cache-to-db conf))
    ; 2、再把当前的buffer写入队列中
    (write-row-buf-to-cache row-buf row-len)
    ; 3、如果所有结束，则把余下的数据也写入数据库
    (if (true? all-done)
      (do (println "所有读写结束，写入剩下的数据。")
          (write-cache-to-db conf)))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
  (if (= @*done-thread-num* @*read-thread-num*)
    true
    false))

(defn reader
  [taks-conf]
  (let [conf (:conf taks-conf)
        sqls (:sqls conf)
        source (:source conf)]
    ; 需要启动的线程总数
    (reset! *read-thread-num* (count sqls))
    (doseq [sql sqls]
      (future
        (try
          (doseq [row (db/query conf sql source)]
            (while (>= (.size DATA-CACHE-QUEUE) QUEEU-LENGTH)
              (log/info "DATA-CACHE-QUEUE is full.")
              (Thread/sleep 1000))
            (.add DATA-CACHE-QUEUE row))
          ; 如果当前线程完成，*done-thread-num* 记数增加 1
          (swap! *done-thread-num* inc)
          (catch Exception e
            (log/error "reader error:" e)
            (reset! *reader-status* IO-ERROR)))))))

(defn writer
  [task-conf]
  (let [conf (:conf task-conf)]
    (try
      (while true
        (if (> (.size DATA-CACHE-QUEUE) 0)
          (do
            (log/info "queue's size =" (.size DATA-CACHE-QUEUE))
            (let [row (.poll DATA-CACHE-QUEUE)
                  queue-size (.size DATA-CACHE-QUEUE)
                  all-done (and (= queue-size 0) (all-read-thread-done?))
                  ; TODO 转化为字符串，去除特殊字符
                  row-str (str (clojure.string/join "\t" row) "\n")
                  row-buf (.getBytes row-str)]
              ; TODO WRITE TO HADOOP
              (write conf row-buf all-done)
              (if (true? all-done)
                (do
                  (reset! *reader-status* IO-DONE)
                  (reset! *writer-status* IO-DONE)))))
          (do
            (log/info "queue's size =" (.size DATA-CACHE-QUEUE) ", all read thread done? " (all-read-thread-done?))
            (Thread/sleep 5))))
      (catch Exception e
        (log/error "writer error:" e)
        (reset! *writer-status* IO-ERROR)))))
