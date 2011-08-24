(ns publishing
  (:import
   [com.rabbitmq.client
    MessageProperties])
  (:require
   [teporingo.publish :as pub]
   [teporingo.core    :as mq]
   [teporingo.broker  :as broker]
   [clj-etl-utils.log :as log])
  (:use
   [teporingo.core           :only [*reply-code* *reply-text* *exchange* *routing-key* *message-properties* *listener* *conn* *props* *body* *active* *confirm-type* *delivery-tag* *multiple* publisher publish]]
   [clj-etl-utils.lang-utils :only [raise]]))


(defn handle-returned-message []
  (log/errorf
   "[publisher] RETURNED: conn=%s code=%s text=%s exchange=%s routing-key:%s props=%s body=%s"
   @*conn*
   *reply-code*
   *reply-text*
   *exchange*
   *routing-key*
   *props*
   (String. *body*)))

(defn handle-confirmed-message []
  (log/infof "[publisher] confirmed message: confirm-type:%s delivery-tag:%s multiple:%s"
             *confirm-type*
             *delivery-tag*
             *multiple*))

(defn handle-flow []
  (log/infof "[publisher] flow: activity:%s" *active*))

(broker/register
 :amqp01
 {:name               :amqp01
  :roles              #{:local}
  ;; :user              "guest"
  ;; :pass              "guest"
  ;; :host              "localhost"
  :port               25671
  :connection-timeout 10
  :reconnect-delay-ms 1000
  ;; :heartbeat-seconds 1
  :vhost              "/"
  ;; :use-confirm        false
  ;; :basic-qos          {:prefetch-size 0 :prefetch-count 1}
  ;; :use-transactions   false
  :listeners          {:return  handle-returned-message}})


(broker/register
 :amqp02
 {:name               :amqp02
  :roles              #{:local}
  ;; :user              "guest"
  ;; :pass              "guest"
  ;; :host              "localhost"
  :port               25672
  :connection-timeout 10
  :reconnect-delay-ms 1000
  ;; :heartbeat-seconds 1
  :vhost              "/"
  ;; :use-confirm        false
  ;; :basic-qos          {:prefetch-size 0 :prefetch-count 1}
  ;; :use-transactions   false
  :listeners          {:return  handle-returned-message}})


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
          (printf "FAILURE[%s] %s\n" ii ex)
          (log/warnf ex "FAILURE[%s] %s\n" ii ex))))))

  )


