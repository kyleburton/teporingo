(defproject com.github.kyleburton/teporingo "2.1.29"
  :description "Teporingo: Rabbit Client Library"
  :url         "http://github.com/kyleburton/teporingo"
  :lein-release {:deploy-via :clojars :scm :git}
  :license {:name         "Eclipse Public License - v 1.0"
            :url          "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments     "same as Clojure"}
  :repositories         {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
  :java-source-paths     ["java"]
  :local-repo-classpath true
  :autodoc {
    :name "Teporingo"
    :page-title "Teporingo: API Documentation"
    :description "Teporingo: HA Rabbit Client Library"
    :web-home "http://kyleburton.github.com/projects/teporingo/"
  }
  :dev-resources-path "dev-resources"

  :profiles             {:dev {:dependencies [[swank-clojure         "1.4.3"]
                                              [lein-marginalia       "0.7.1"]]}
                         :1.2 {:dependencies [[org.clojure/clojure   "1.2.1"]
                                              [org.clojure/data.json "0.2.2"]]}
                         :1.3 {:dependencies [[org.clojure/clojure   "1.3.0"]
                                             [org.clojure/data.json  "0.2.3"]]}
                         :1.4 {:dependencies [[org.clojure/clojure   "1.4.0"]
                                             [org.clojure/data.json  "0.2.3"]]}
                         :1.5 {:dependencies [[org.clojure/clojure   "1.5.1"]
                                             [org.clojure/data.json  "0.2.3"]]}
                         :1.6 {:dependencies [[org.clojure/clojure   "1.6.0-master-SNAPSHOT"]
                                             [org.clojure/data.json  "0.2.3"]]}}
  :aliases              {"all" ["with-profile" "dev,1.2:dev,1.3:dev,1.4:dev,1.5:dev,1.6"]}
  :global-vars          {*warn-on-reflection* true}
  :dependencies         [
                         [log4j/log4j                          "1.2.14"]
                         [redis.clients/jedis                  "2.1.0"]
                         [com.rabbitmq/amqp-client             "3.0.2"]
                         [com.github.kyleburton/clj-etl-utils  "1.0.76"]
                         [com.relaynetwork/clorine             "1.0.17"]
                         ]
)
