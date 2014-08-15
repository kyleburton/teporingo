(ns publishing
  (:import
   [com.rabbitmq.client
    MessageProperties])
  (:require
   [teporingo.publish :as pub]
   [teporingo.core    :as mq]
   [clojure.tools.logging :as log])
  (:use
   [teporingo.core           :only [*reply-code* *reply-text* *exchange* *routing-key* *message-properties* *listener* *conn* *props* *body* *active* *confirm-type* *delivery-tag* *multiple*]]
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

(def *amqp-config*
     {:name               "*none*"
      :port               5672
      :connection-timeout 10
      :reconnect-delay-ms 1000
      :queue-name         "foofq"
      :vhost              "/"
      :exchange-name      "/foof"
      :bindings           [{:routing-key        ""}]
      :closed?            true
      :listeners          {:return  handle-returned-message}})


(pub/register-amqp-broker-cluster
 :local-rabbit-cluster
 [(assoc *amqp-config*
    :name "rabbit01"
    :port 25671)
  (assoc *amqp-config*
    :name "rabbit02"
    :port 25672)])


(defonce *publisher* (pub/make-publisher :local-rabbit-cluster))

(comment
  (do
    (mq/close-connection! *publisher*)
    (def *publisher* (pub/make-publisher :local-rabbit-cluster)))

  (pub/disable-broker  :local-rabbit-cluster)
  (pub/enable-broker   :local-rabbit-cluster)
  (pub/broker-enabled? :local-rabbit-cluster)

  (time
   (dotimes [ii 10]
     (try
      (pub/publish
       *publisher*
       "/foof"
       ""
       true  ;; mandatory
       false ;; immediate
       MessageProperties/PERSISTENT_TEXT_PLAIN
       (str "hello there:" ii)
       2)
      (printf "SUCCESS[%s]: Published to at least 1 broker.\n" ii)
      (catch Exception ex
        (printf "FAILURE[%s] %s\n" ii ex)
        (log/warnf ex "FAILURE[%s] %s\n" ii ex)))))

  )



(time
 (dotimes [ii 1000]
  (try
   (pub/publish
    *publisher*
    "/foof"
    ""
    true  ;; mandatory
    false ;; immediate
    MessageProperties/PERSISTENT_TEXT_PLAIN
    (str "hello there:" ii)
    2)
   (printf "SUCCESS[%s]: Published to at least 1 broker.\n" ii)
   (catch Exception ex
    (printf "FAILURE[%s] %s\n" ii ex)
    (log/warnf ex "FAILURE[%s] %s\n" ii ex)))))

