(ns publishing
  (:import
   [com.rabbitmq.client
    MessageProperties])
  (:require
   [teporingo.publish :as pub]
   [teporingo.core    :as mq]
   [clj-etl-utils.log :as log])
  (:use
   [clj-etl-utils.lang-utils :only [raise]]))




(pub/register-amqp-broker-cluster
 :local-rabbit-cluster
 [{:name "rabbit01"
   :port 25671
   :use-confirm true
   :connection-timeout 10
   :queue-name "foofq"
   :routing-key ""
   :vhost "/"
   :exchange-name "/foof"
   :closed? true}
  {:name "rabbit02"
   :port 25672
   :connecton-timeout 10
   :use-confirm true
   :queue-name "foofq"
   :routing-key "#"
   :vhost "/"
   :exchange-name "/foof"
   :closed? true}])

(def *publisher* (pub/make-publisher :local-rabbit-cluster))

(doseq [conn (:connections *publisher*)]
  (mq/attach-listener! conn
                       (mq/make-return-listener
                        conn
                        {:handle-return
                         (fn []
                           (let [msg (format "RETURNED: conn=%s code=%s text=%s exchange=%s routing-key:%s props=%s body=%s"
                                             @*conn*
                                             ;; TODO/HERE/NB complete conversion to bindings...
                                             reply-code
                                             reply-text
                                             exchange
                                             routing-key
                                             props
                                             (String. body))]
                             (log/errorf msg)))})))

(comment
  (mq/close-connection! *publisher*)

  (dotimes [ii 1]
    (try
     (pub/publish
      *publisher*
      "/foof"
      ""
      true
      false
      MessageProperties/PERSISTENT_TEXT_PLAIN
      (.getBytes (str "hello there:" ii))
      2)
     (printf "SUCCESS[%s]: Published to at least 1 broker.\n" ii)
     (catch Exception ex
       (printf "FAILURE[%s] %s\n" ii ex)
       (log/warnf ex "FAILURE[%s] %s\n" ii ex))))

  )




