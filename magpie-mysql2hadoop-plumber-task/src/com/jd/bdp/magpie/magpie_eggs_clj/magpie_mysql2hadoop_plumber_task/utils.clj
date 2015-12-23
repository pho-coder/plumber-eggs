(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.utils
  (:use [com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap])
  (:require [clojure.tools.logging :as log]
            [clj-zookeeper.zookeeper :as zk]
            [com.jd.bdp.magpie.util.utils :as magpie-utils]))

(defn- split-task-id
  [task-id]
  ; [prefix albatross-id job-id uuid]
  (clojure.string/split task-id SEPARATOR))

(defn get-albatross-id
  [task-id]
  ; (= 2 (nth [0 1 2 3] 2))
  (nth (split-task-id task-id) 1))

(defn get-job-id
  [task-id]
  ; (= 2 (nth [0 1 2 3] 2))
  (nth (split-task-id task-id) 2))

(defn check-task-valid?
  [task-id]
  (let [[prefix albatross-id job-id uuid] (clojure.string/split task-id SEPARATOR)]
    (if-not (= prefix "plumber")
      (do (log/error "prefix:" prefix "NOT plumber!")
          false)
      (let [albatrosses-path "/albatross/albatrosses/"
            albatross-node (str albatrosses-path albatross-id)]
        (if-not (zk/check-exists? albatross-node)
          (do (log/error "albatross:" albatross-id "NOT exists!")
              false)
          (let [jobs-path "/albatross/jobs/"
                job-node (str jobs-path job-id)]
            (if-not (zk/check-exists? job-node)
              (do (log/error "job:" job-id "NOT exists!")
                  false)
              (let [job-info (magpie-utils/bytes->map (zk/get-data job-node))
                    albatross (get job-info "albatross")]
                (if-not (= albatross albatross-id)
                  (do (log/error "albatross:" albatross "NOT" albatross-id)
                      false)
                  true)))))))))
