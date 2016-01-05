(defproject com.jd.bdp.magpie.magpie-eggs-clj/magpie-mysql2hadoop-plumber-task "0.0.1-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.jd.bdp.magpie/magpie-framework-clj "0.1.0-SNAPSHOT"]
                 [thrift-clj "0.3.0"]
                 [clj-zookeeper "0.2.0-SNAPSHOT"]
                 [mysql-java "5.1.21"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [org.apache.hadoop/hadoop-common "2.2.0"]
                 [org.apache.hadoop/hadoop-hdfs "2.2.0"]
                 [com.jd.bdp.magpie/magpie-utils "0.1.3-SNAPSHOT"]]
  :main ^:skip-aot com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :plugins [[lein-thriftc "0.2.3"]]
  :hooks [leiningen.thriftc]
  :resource-paths ["resources"]
  :thriftc {:path "thrift"
            :source-paths ["src/thrift"]
            :java-gen-opts "beans,hashcode,nocamel"
            :force-compile true})
