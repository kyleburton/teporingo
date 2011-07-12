(ns consumer
  (:require
   [clj-etl-utils.log :as log])
  (:use
   teporingo.client
   [teporingo.core           :only [*body* *consumer-tag* *envelope* *message-id* *message-timestamp*]]
   [clj-etl-utils.lang-utils :only [raise]]))

(def *amqp01-config*
     {:port                          25671
      :vhost                         "/"
      :exchange-name                 "/foof"
      :queue-name                    "foofq"
      :heartbeat-seconds             5
      :restart-on-connection-closed? true
      :reconnect-delay-ms            1000
      :ack?                          true})

(def *amqp02-config*
     (assoc *amqp01-config*
       :port 25672))

(defn handle-amqp-delivery []
  (try
   (log/infof "CONSUMER: got a delivery")
   (log/infof "CONSUMER: [%s/%s|%s] msg-id[%s]:%s body='%s'" *consumer-tag* (.getRoutingKey *envelope*) (.getDeliveryTag *envelope*) *message-timestamp* *message-id* *body*)
   (catch Exception ex
     (log/errorf ex "Consumer Error: %s" ex))))

(register-consumer
 :foof01
 *amqp01-config*
 {:delivery handle-amqp-delivery})


(register-consumer
 :foof02
 *amqp02-config*
 {:delivery handle-amqp-delivery})

(comment

  (add-consumer :foof01)
  (add-consumer :foof02)
  (stop-one :foof01)
  (stop-all :foof01)
  (stop-all :foof02)
  (stop-all)

  (consumer-type-enabled? :foof01)
  (consumer-type-enabled? :foof02)
  (disable-consumer-type :foof01)
  (enable-consumer-type  :foof01)


  consumer-type-registry

  active-consumers

  (clojure.contrib.pprint/pprint @active-consumers)

  (let [ex (agent-error consumer-restart-agent)]
    (log/infof ex "Error: %s" ex))

  (clear-agent-errors consumer-restart-agent)



  )
