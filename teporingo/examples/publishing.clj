(ns publishing
  (:import
   [com.rabbitmq.client
    MessageProperties])
  (:require
   [teporingo.publish :as pub]
   [teporingo.core    :as mq]
   [teporingo.broker  :as broker]
   [clojure.tools.logging :as log])
  (:use
   [teporingo.core           :only [*reply-code* *reply-text* *exchange* *routing-key* *message-properties* *listener* *conn* *props* *body* *active* *confirm-type* *delivery-tag* *multiple* publisher publish]]
   [clj-etl-utils.lang-utils :only [raise]]))

(load-file "examples/broker-config.clj")

(pub/register
 :wocal_wabbits
 {:name               :wocal_wabbits
  :broker-roles       #{:local}
  :min-published-to   1
  :queues             [{:name       "foofq"
                        :durable    true
                        :exclusive  false
                        :autodelete false
                        :bindings   [{:routing-key        ""}]}]
  :exchange-name      "/foof"
  :num-retries        2
  :routing-key        (fn [body] "")
  :message-properties MessageProperties/PERSISTENT_TEXT_PLAIN
  :immediate          false
  :mandatory          true
  :serializer         (fn [body] (.getBytes (str body)))
  :breaker-type       :agent})

(comment

  (time
   (pub/with-publisher :wocal_wabbits
     (dotimes [ii 1]
       (try
        (publish (str "hello there:" ii))
        (printf "SUCCESS[%s]: Published to at least 1 broker.\n" ii)
        (catch Exception ex
          (.printStackTrace ex)
          (printf "FAILURE[%s] %s\n" ii ex)
          (log/warnf ex "FAILURE[%s] %s\n" ii ex))))))

  )


