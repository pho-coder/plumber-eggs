(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-test-plumber-task.client
  (:require [clojure.tools.logging :as log]
            [thrift-clj.core :as thrift])
  (:import [org.apache.thrift.transport TTransportException]))

(def ^:dynamic *albatross-client* (atom nil))
(def ^:dynamic *reset-albatross-client* (atom nil))

(thrift/import
 (:clients com.jd.bdp.magpie.albatross.generated.Albatross))

(defn get-albatross-client
  [ip port]
  (log/info "get albatross client!")
  (try
    (reset! *albatross-client* (thrift/connect! Albatross [ip port]))
    (catch Throwable e
      (log/error "get-albatross-client" e)
      (reset! *reset-albatross-client* true))))

(defn get-conf
  [job-id task-id]
  (try
    (let [conf (Albatross/getTaskConf @*albatross-client* job-id task-id)]
      conf)
    (catch TTransportException e
      (log/error e)
      (reset! *reset-albatross-client* true))))

(defn heartbeat
  [job-id task-id status]
  (try
    (Albatross/islandHeartbeat @*albatross-client* job-id task-id status)
    (catch TTransportException e
      (log/error e)
      (reset! *reset-albatross-client* true))))
