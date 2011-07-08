(ns teporingo.client
  (:import
   [com.rabbitmq.client
    ConnectionFactory
    Connection
    Channel
    Consumer
    AlreadyClosedException
    ReturnListener
    ConfirmListener
    MessageProperties
    Envelope
    AMQP$BasicProperties
    ShutdownSignalException])
  (:require
   [clj-etl-utils.log :as log])
  (:use
   teporingo.core
   [clj-etl-utils.lang-utils :only [raise]]))




(defn make-consumer [conn handlers]
  (let [default-handler (fn [& args]
                          nil)
        handlers (merge {:cancel default-handler
                         :consume default-handler
                         :recover default-handler
                         :shutdown default-handler}
                        handlers)
        {cancel :cancel
         consume :consume
         delivery :delivery
         recover :recover
         shutdown :shutdown} handlers
        consumer (reify
                  Consumer
                  (^void handleCancelOk [^Consumer this ^String consumer-tag]
                         (cancel conn this consumer-tag))
                  (^void handleConsumeOk [^Consumer this ^String consumer-tag]
                         (consume conn this consumer-tag))
                  (^void handleDelivery [^Consumer this ^String consumer-tag ^Envelope envelope ^AMQP$BasicProperties properties ^bytes body]
                         (delivery conn this consumer-tag envelope properties body))
                  (^void handleShutdownSignal [^Consumer this ^String consumer-tag ^ShutdownSignalException sig]
                         (shutdown conn this consumer-tag sig))
                  (^void handleRecoverOk [^Consumer this]
                         (recover conn this)))]
    {:conn conn
     :type :consumer
     :listener consumer}))

(defn shutdown-consumer! [consumer]
  (doseq [listener (:listeners consumer)]
    (cond
      (= :consumer (:type listener))
      (.basicCancel
       (:channel @(:conn listener))
       (:consumer-tag @(:conn listener ""))))
    (close-connection! listener))
  consumer)

(defn shutdown-consumer-quietly! [consumer]
  (try
   (shutdown-consumer! consumer)
   (catch Exception ex
     nil))
  (doseq [listener (:listeners consumer)]
    (close-connection! (:conn listener)))
  consumer)

(defn start-consumer! [consumer]
  (shutdown-consumer-quietly! consumer)
  (doseq [listener (:listeners consumer)]
    (let [conn          (:conn listener)
          channel       (:channel   @conn)
          exchange-name (:exchange-name  @conn "")
          queue-name    (:queue-name @conn "")
          routing-key   (:routing-key @conn *default-routing-key*)
          listener-type (:type listener)]
      (ensure-connection! conn)
      (exchange-declare! conn)
      (queue-declare!    conn)
      (queue-bind!       conn)
      (attach-listener!  conn listener)))
  consumer)

(comment

    (def *c1*
       (let [conn (atom {:port 25672
                         :vhost           "/"
                         :exchange-name   "/foof"
                         :queue-name      "foofq"})]
         {:listeners
          [(make-consumer
            conn
            {:delivery
             (fn [conn consumer consumer-tag envelope properties body]
               (try
                (log/infof "CONSUMER: got a delivery")
                (let [msg (String. body)]
                  (log/infof "CONSUMER: body='%s'" msg))
                (.basicAck (:channel @conn)
                           (.getDeliveryTag envelope) ;; delivery tag
                           false)                     ;; multiple
                (catch Exception ex
                  (log/errorf ex "Consumer Error: %s" ex))))})
           (make-default-return-listener conn)]}))


  (start-consumer! *c1*)

  (shutdown-consumer-quietly! *c1*)

  (def *c2*
       (let [conn (atom {:port 25671
                         :vhost           "/"
                         :exchange-name   "/foof"
                         :queue-name      "foofq"})]
         {:listeners
          [(make-consumer
            conn
            {:delivery
             (fn [conn consumer consumer-tag envelope properties body]
               (try
                (log/infof "CONSUMER: got a delivery")
                (let [msg (String. body)]
                  (log/infof "CONSUMER: body='%s'" msg))
                (.basicAck (:channel @conn)
                           (.getDeliveryTag envelope) ;; delivery tag
                           false)                     ;; multiple
                (catch Exception ex
                  (log/errorf ex "Consumer Error: %s" ex))))})
           (make-default-return-listener conn)]}))

  (start-consumer! *c2*)

  (shutdown-consumer-quietly! *c2*)



)