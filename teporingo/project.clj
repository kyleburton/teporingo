(defproject 
  rabbit-client "1.0.0-SNAPSHOT"
  :description "HA Rabbit Client Library"
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]]
  :dev-resources-path "dev-resources"
  :local-repo-classpath true
  :java-source-path [["java"]]
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [log4j/log4j "1.2.14"]
                 [clj-yaml "0.3.0-SNAPSHOT"]
                 [redis.clients/jedis "2.0.0"]
                 [com.rabbitmq/amqp-client "2.5.0"]
                 [org.clojars.kyleburton/clj-etl-utils "1.0.34"]
                 [com.relaynetwork/clorine "1.0.4"]])

