(ns consumer
  (:require
   teporingo.redis
   [clj-etl-utils.log :as log])
  (:use
   teporingo.client
   [teporingo.core           :only [*body* *consumer-tag* *envelope* *message-id* *message-timestamp*]]
   [clj-etl-utils.lang-utils :only [raise]]))

(load-file "examples/broker-config.clj")

(defn handle-amqp-delivery []
  (try
   (log/infof "CONSUMER[%s]: got a delivery" *consumer-tag*)
   #_(let [val (int (* 3000 (.nextDouble (java.util.Random.))))]
       (log/infof "CONSUMER[%s]: simulating 'work' for %sms" *consumer-tag* val)
       (Thread/sleep val))
   (log/infof "CONSUMER[%s]: [%s|%s] msg-id[%s]:%s body='%s'" *consumer-tag* (.getRoutingKey *envelope*) (.getDeliveryTag *envelope*) *message-timestamp* *message-id* *body*nn)
   (catch Exception ex
     (log/errorf ex "Consumer Error: %s" ex))))

(teporingo.redis/register-redis-pool :local)

(def *consumer-config*
     {:exchange-name                 "/foof"
      :queue-name                    "foofq"
      :restart-on-connection-closed? true
      :reconnect-delay-ms            1000
      :ack?                          true
      :bindings                      [{:routing-key ""}]
      :handlers
      {:delivery
       (teporingo.redis/make-deduping-delivery-fn
        {:redis-instance :local
         :timeout 1000}
        :foof
        handle-amqp-delivery)}})

(register-consumer
 :foof01 ;; consumer name
 :amqp01 ;; registered broker name
 *consumer-config*)

(register-consumer
 :foof02 ;; consumer name
 :amqp02 ;; registered broker name
 *consumer-config*)

(comment

  (add-consumer :foof01)
  (add-consumer :foof02)
  (stop-one :foof01)
  (stop-all :foof01)
  (stop-all :foof02)
  (stop-all)

  (enabled? :foof01)
  (enabled? :foof02)
  (disable :foof01)
  (enable  :foof01)


  consumer-registry

  active-consumers

  (clojure.contrib.pprint/pprint @active-consumers)

  (let [ex (agent-error consumer-restart-agent)]
    (log/infof ex "Error: %s" ex))

  (clear-agent-errors consumer-restart-agent)



  )
