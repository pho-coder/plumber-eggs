(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap)

(use 'clojure.set)

(def STATUS-INIT "init")
(def STATUS-RUNNING "running")
(def STATUS-FINISH "finish")
(def STATUS-STOP "stop")

(def ALBATROSSES-PART "/albatross/albatrosses/")

(def QUEEU-LENGTH 100)

(def SEPARATOR #"\*p\*")
