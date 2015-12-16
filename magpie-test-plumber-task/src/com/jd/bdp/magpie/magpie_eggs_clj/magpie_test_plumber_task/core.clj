(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-test-plumber-task.core
  (:gen-class)
  (:use [com.jd.bdp.magpie.magpie-eggs-clj.magpie-test-plumber-task.bootstrap])
  (:require [clojure.tools.logging :as log]
            [clj-zookeeper.zookeeper :as zk]
            [com.jd.bdp.magpie.magpie-framework-clj.task-executor :as task-executor]
            [com.jd.bdp.magpie.util.utils :as magpie-utils]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-test-plumber-task.utils :as utils]
            [com.jd.bdp.magpie.magpie-eggs-clj.magpie-test-plumber-task.client :as client]))

;; {:id albatross :ip ip :port port}
(def ^:dynamic albatross (atom nil))
;; {:job-id job-id :task-id task-id :uuid :conf conf}
(def ^:dynamic task (atom nil))

(def ^:dynamic tmp-start-time (atom nil))

(defn prepare-fn
  [task-id]
  (if (utils/check-task-valid? task-id)
    (log/info "task id valid ok!" task-id)
    (do (log/info "task id NOT valid!" task-id)
        (System/exit 0)))
  (let [[prefix albatross-id job-id uuid] (clojure.string/split task-id SEPARATOR)
        albatrosses-path "/albatross/albatrosses/"
        albatross-node (str albatrosses-path albatross-id)
        albatross-info (magpie-utils/bytes->map (zk/get-data albatross-node))
        albatross-ip (get albatross-info "ip")
        albatross-port (get albatross-info "port")]
    (reset! albatross {:id albatross-id
                       :ip albatross-ip
                       :port albatross-port})
    (log/info @albatross)
    (client/get-albatross-client (:ip @albatross) (:port @albatross))
    (let [conf (client/get-conf job-id task-id)]
      (reset! task {:job-id job-id
                    :task-id task-id
                    :uuid uuid
                    :conf conf})
      (log/info "task:" @task))
    (client/heartbeat (:job-id @task) (:task-id @task) STATUS-INIT)
    (log/info task-id "is preparing!")
    (reset! tmp-start-time (magpie-utils/current-time-millis))))

(defn run-fn [task-id]
  (log/info (magpie-utils/current-time-millis))
  (log/info "run")
  (Thread/sleep 3000)
  (if @client/*reset-albatross-client*
    (do (client/get-albatross-client @albatross)
        (reset! client/*reset-albatross-client* false)))
  (client/heartbeat (:job-id @task) (:task-id @task) (if (> (- (magpie-utils/current-time-millis) @tmp-start-time) 20000)
                                                       STATUS-FINISH
                                                       STATUS-RUNNING)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (log/info "Hello, World!")
  (try
    (task-executor/execute run-fn :prepare-fn prepare-fn)
    (catch Throwable e
      (log/error e)
      (log/info "bye"))))
