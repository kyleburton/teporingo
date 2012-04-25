(defproject com.github.kyleburton/teporingo "2.1.7"
  :url         "http://github.com/kyleburton/teporingo"
  :license {:name         "Eclipse Public License - v 1.0"
            :url          "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments     "same as Clojure"}
  :description "Teporingo: HA Rabbit Client Library"
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]
                     [lein-margauto "1.0.0"]
                     [lein-exec "0.1"]]
  :dev-resources-path "dev-resources"
  :local-repo-classpath true
  :java-source-path "java"
  :lein-release {:deploy-via :clojars :scm :git}
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [log4j/log4j "1.2.14"]
                 ; [clj-yaml "0.3.0-SNAPSHOT"]
                 [redis.clients/jedis "2.0.0"]
                 [com.rabbitmq/amqp-client "2.5.0"]
                 [org.clojars.kyleburton/clj-etl-utils "1.0.39"]
                 [com.relaynetwork/clorine "1.0.4"]]
  :autodoc {
    :name "Teporingo"
    :page-title "Teporingo: API Documentation"
    :description "Teporingo: HA Rabbit Client Library"
    :web-home "http://kyleburton.github.com/projects/teporingo/"
  })

