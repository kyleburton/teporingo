(ns teporingo.client
  (:import
   [com.rabbitmq.client
    Consumer
    Envelope
    AMQP$BasicProperties
    ShutdownSignalException])
  (:require
   [clj-etl-utils.log :as log])
  (:use
   teporingo.core
   [clj-etl-utils.lang-utils :only [raise]]))


;; NB: when we hit clojure 1.3, use ^:dynamic
(def *conn*         nil)
(def *consumer*     nil)
(def *consumer-tag* nil)
(def *envelope*     nil)
(def *properties*   nil)
(def *body*         nil)
(def *sig*          nil)

(defn ack-message []
  (.basicAck (:channel        @*conn*)
             (.getDeliveryTag *envelope*) ;; delivery tag
             false))


;; TODO: throw if they didn't provide a :delivery handler?
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
                         (binding [*conn*         conn
                                   *consumer*     this
                                   *consumer-tag* consumer-tag]
                           (cancel))
                         (cancel))
                  (^void handleConsumeOk [^Consumer this ^String consumer-tag]
                         (binding [*conn*         conn
                                   *consumer*     this
                                   *consumer-tag* consumer-tag]
                           (consume)))
                  (^void handleDelivery [^Consumer this ^String consumer-tag ^Envelope envelope ^AMQP$BasicProperties properties ^bytes body]
                         (binding [*conn*         conn
                                   *consumer*     this
                                   *consumer-tag* consumer-tag
                                   *envelope*     envelope
                                   *properties*   properties
                                   *body*         body]
                           (delivery)
                           (if (:ack? @conn)
                             (ack-message))))
                  (^void handleShutdownSignal [^Consumer this ^String consumer-tag ^ShutdownSignalException sig]
                         (binding [*conn*         conn
                                   *consumer*     this
                                   *consumer-tag* consumer-tag
                                   *sig*          sig]
                           (shutdown)))
                  (^void handleRecoverOk [^Consumer this]
                         (binding [*conn*         conn
                                   *consumer*     this]
                           (recover))))]

    {:conn conn
     :type :consumer
     :listener consumer}))

(defn shutdown-consumer! [consumer]
  (try
   (cond
     (= :consumer (:type consumer))
     (.basicCancel
      (:channel      @(:conn consumer))
      (:consumer-tag @(:conn consumer) ""))
     :else
     (raise "Error: don't know how to shutdown consumer of type=%s, only :consumer is supported." (:type consumer)))
   (finally
    (close-connection! (:conn consumer))))
  consumer)

(defn shutdown-consumer-quietly! [consumer]
  (try
   (shutdown-consumer! consumer)
   (catch Exception ex
     (log/debugf ex "Error shutting down consumer: %s" ex)))
  consumer)

(defn start-consumer! [consumer]
  (shutdown-consumer-quietly! consumer)
  (let [conn          (:conn           consumer)]
    (ensure-connection! conn)
    (exchange-declare!  conn)
    (queue-declare!     conn)
    (queue-bind!        conn)
    (attach-listener!   conn consumer))
  consumer)

(defonce consumer-type-registry (atom {}))

(defn register-consumer [type amqp-credentials handler-functions]
  (swap! consumer-type-registry
         assoc
         type
         {:type              type
          :amqp-credentials  amqp-credentials
          :handler-functions handler-functions}))

(defn lookup-conumer [type]
  (let [config (type @consumer-type-registry)]
    (if-not config
      (raise "Error: unregistered consumer type: %s" type))
    config))

(defonce active-consumers (atom {}))

(defn add-consumer [type]
  (let [config        (lookup-conumer type)
        consumers-vec (type @active-consumers [])
        conn          (atom (:amqp-credentials  config))
        consumer      (make-consumer conn (:handler-functions config))]
    (start-consumer! consumer)
    (swap! active-consumers
           assoc
           type
           (conj consumers-vec consumer))))

(defn stop-all [type]
  (dosync
   (doseq [consumer (type @active-consumers)]
     (shutdown-consumer-quietly! consumer))
   (swap! active-consumers
          assoc
          type
          [])))

(defn stop-one [type]
  (dosync
   (let [[consumer & rest] (type @active-consumers)]
     (shutdown-consumer-quietly! consumer)
     (swap! active-consumers
            assoc
            type
            rest)))
  (count (type @active-consumers)))

(comment
  (register-consumer
   :foof
   {:port            25672
    :vhost           "/"
    :exchange-name   "/foof"
    :queue-name      "foofq"
    :ack?            true}
   {:delivery
    (fn []
      (try
       (log/infof "CONSUMER: got a delivery")
       (let [msg (String. *body*)]
         (log/infof "CONSUMER: body='%s'" msg))
       (catch Exception ex
         (log/errorf ex "Consumer Error: %s" ex))))})

  (add-consumer :foof)
  (stop-one :foof)
  (stop-all :foof)

  consumer-type-registry

  active-consumers

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