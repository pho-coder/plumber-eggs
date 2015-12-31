(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap
  (:import (java.util.concurrent LinkedBlockingQueue)))

(use 'clojure.set)

(def STATUS-INIT "init")
(def STATUS-RUNNING "running")
(def STATUS-FINISH "finish")
(def STATUS-STOP "stop")

(def ALBATROSSES-PART "/albatross/albatrosses/")

(def SEPARATOR #"\*p\*")

(def DATA-BUFFER-MAX-SIZE (* 256 1))

(def QUEEU-LENGTH 100)
(def ^:dynamic DATA-CACHE-QUEUE (LinkedBlockingQueue. QUEEU-LENGTH))

(def IO-DONE "done")
(def IO-ERROR "error")

(def BASE-CONF {:source "mysql"
                :target "mysql2hadoop.log."
                :host "127.0.0.1"
                :sql "select * from Persons"
                :sqls ["select * from Users" "select * from Persons"]
                :db-name "User"
                :user "xiao"
                :password "mysql"
                :jar "magpie-mysql2hadoop-plumber-task-0.0.1-SNAPSHOT-standalone.jar"
                :klass "com.jd.bdp.magpie.magpie_eggs_clj.magpie_mysql2hadoop_plumber_task.core"
                :group "default"
                :type "memory"})

; "plumber*p*albatross-test-0*p*test-job*p*task-0"