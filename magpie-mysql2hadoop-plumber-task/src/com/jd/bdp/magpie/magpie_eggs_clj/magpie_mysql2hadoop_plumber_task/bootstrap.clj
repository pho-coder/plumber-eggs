(ns com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.bootstrap)

(use 'clojure.set)

(def STATUS-INIT "init")
(def STATUS-RUNNING "running")
(def STATUS-FINISH "finish")
(def STATUS-STOP "stop")

(def ALBATROSSES-PART "/albatross/albatrosses/")

(def QUEEU-LENGTH 100)

(def SEPARATOR #"\*p\*")

(def BASE-CONF {:source "mysql"
                :target "hadoop"
                :sql "select * from a"
                :jar "magpie-mysql2hadoop-plumber-task-0.0.1-SNAPSHOT-standalone.jar"
                :klass "com.jd.bdp.magpie.magpie_eggs_clj.magpie_mysql2hadoop_plumber_task.core"
                :group "default"
                :type "memory"})

; "plumber*p*albatross-test-0*p*test-job*p*task-0"