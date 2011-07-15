;; # AMQP Consumer Module
;;
;; This module provides a wrapper around the
;; [RabbitMQ](http://rabbitmq.org/ "RabbitMQ") client library's
;; Consumer class.
;;
;; ## Consumer Registry
;;
;; The module includes a registry for consumer types and controls that
;; allow you to dynamically add (start) and remove (shutdown)
;; consumers.
;;

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

;; Flag to allow consuemrs of a specific type to be disabled - no more
;; will be able to be started.  Types can be temporarily disabled
;; without having to be removed from the registry.
(defonce *disabled-consumers-by-type* (atom #{}))

;; Disable a consumer of a specific type.
(defn disable-consumer-type [type]
  (swap! *disabled-consumers-by-type* conj type))

;; Enable a consumer of a specific type.
(defn enable-consumer-type [type]
  (swap! *disabled-consumers-by-type* disj type))

;; Test if a consumer type is enabled.
(defn consumer-type-enabled? [type]
  (not (contains? @*disabled-consumers-by-type* type)))

;; Acknowledge a message to the channel.  This will notify the AMQP
;; Broker to mark the message as delivered and handled so that it will
;; not be returned or delivered again.
(defn ack-message []
  (.basicAck (:channel        @*conn*)
             (.getDeliveryTag *envelope*)
             false))

;; ## Consumer Management Agent Starting and restarting consumers is
;; managed by an agent.  This decouples the act of initiating the
;; restart from the consumer that was being shut down - allowing it to
;; be completely shut down.
(defonce consumer-restart-agent (agent {}))
(declare start-consumer!)

;; This is a registry of the currently active consumers, keyed by
;; `consumerTag`.
(defonce active-consumers (atom {}))

;; Create, but do not start, an AMQP consumer.
;;
;; `type` must be a registered consumer type.  See `register-consumer`.
;;
;; `conn` must be an atom containing a map to hold the connection and
;; configuration for the connection.
;;
;; `handlers` must be a dispatch map for the event handlers that
;; implement the consumer.  Valid entries are:
;;
;; * `:delivery` *required*
;;    to handle delivery of a message from the broker.
;; * `:cancel`
;; * `:recover`
;; * `:shutdown`
;;
;; Note that though a `:shutdown` handler may be specified, Teporingo
;; implements additional behavior on shutdown if the consumer is
;; configured to be restarted (via the key
;; `:restart-on-connection-closed?`): it will send a restart request
;; to the agent in the event the consumer dies.

(defn parse-long [thing]
  (try
   (Long/parseLong thing)
   (catch NumberFormatException ex
     0)))

(defn make-consumer [type conn handlers]
  (if-not (:delivery handlers)
    (raise "Error: can not create a consumer without a :delivery handler specified; passed: %s"
           (vec (keys handlers))))
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
        the-consumer (atom     {:conn            conn
                                :registered-type type
                                :type            :consumer
                                :listener        nil})
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
                           (swap! conn assoc :consumer-tag consumer-tag)
                           (swap! active-consumers
                                  update-in
                                  [type]
                                  assoc
                                  consumer-tag
                                  @the-consumer)
                           (consume)))
                  (^void handleDelivery [^Consumer this
                                         ^String consumer-tag
                                         ^Envelope envelope
                                         ^AMQP$BasicProperties properties
                                         ^bytes body]
                         (let [raw-body          body
                               [teporing-hdr-magic message-id message-timestamp body]
                               (split-body-and-msg-id (String. raw-body))]
                           (binding [*conn*         conn
                                     *consumer*     this
                                     *consumer-tag* consumer-tag
                                     *envelope*     envelope
                                     *properties*   properties
                                     *body*         body
                                     *raw-body*     raw-body
                                     *message-id*   message-id
                                     *message-timestamp* (parse-long message-timestamp)]
                             (delivery)
                             (if (:ack? @conn)
                               (ack-message)))))
                  (^void handleShutdownSignal [^Consumer this
                                               ^String consumer-tag
                                               ^ShutdownSignalException sig]
                         (binding [*conn*         conn
                                   *consumer*     this
                                   *consumer-tag* consumer-tag
                                   *sig*          sig]
                           (shutdown)
                           (log/infof "Consumer[%s/%s] was shut down %s" type consumer-tag sig)
                           (when (:restart-on-connection-closed? @conn)
                             (log/infof "Consumer[%s/%s] will be restarted: conn=%s consumer=%s"
                                        type consumer-tag @conn @the-consumer)
                             (start-consumer! @the-consumer consumer-tag))))
                  (^void handleRecoverOk [^Consumer this]
                         (binding [*conn*         conn
                                   *consumer*     this]
                           (recover))))]
    (swap! the-consumer assoc :listener consumer)
    @the-consumer))

(defn shutdown-consumer! [consumer]
  (try
   (cond
     (= :consumer (:type consumer))
     (when (:channel @(:conn consumer))
       (.basicCancel
        (:channel      @(:conn consumer))
        (:consumer-tag @(:conn consumer) "")))
     :else
     (raise "Error: don't know how to shutdown consumer of type=%s, only :consumer is supported. in %s"
            (:type consumer) consumer))
   (finally
    (close-connection! (:conn consumer))))
  consumer)

(defn shutdown-consumer-quietly! [consumer]
  (try
   (shutdown-consumer! consumer)
   (catch Exception ex
     (log/debugf ex "[IGNORE] Error shutting down consumer: %s" ex)))
  consumer)

(declare stop-consumer-with-tag)

(defn agent-start-consumer!
  ([state consumer]
     (try
      (log/debugf "agent-start-consumer! type=%s" (:registered-type consumer))
      (shutdown-consumer-quietly! consumer)
      (log/debugf "agent-start-consumer! ensured shut down, about to start.  Type=%s"
                  (:registered-type consumer))
      (let [conn               (:conn           consumer)]
        (ensure-connection! conn)
        (exchange-declare!  conn)
        (queue-declare!     conn)
        (queue-bind!        conn)
        (attach-listener!   conn consumer))
      (log/debugf "agent-start-consumer! swapping active-consumers for type=%s"
                  (:registered-type consumer))
      (catch Exception ex
        (let [conn               (:conn           consumer)
              reconnect-delay-ms (:reconnect-delay-ms @conn 250)]
          (log/infof ex "agent-start-consumer! error during connect: %s, will re-attempt in %sms"
                     ex reconnect-delay-ms)
          (delay-by
              reconnect-delay-ms
            (start-consumer! consumer)))))
     state)
  ([state consumer consumer-tag]
     (log/debugf "agent-start-consumer! type:%s tag:%s" (:registered-type consumer) consumer-tag)
     (stop-consumer-with-tag (:registered-type consumer) consumer-tag)
     (start-consumer! consumer)
     state))

(defn start-consumer!
  ([consumer]
     (log/infof "consumer: %s" consumer)
     (if (and
          (consumer-type-enabled? (:registered-type consumer))
          (not (:all-stop @(:conn consumer))))
       (send-off consumer-restart-agent agent-start-consumer! consumer)
       (log/infof "NOT starting consumer[%s]: consumer-type-enabled?:%s %s"
                  (:registered-type consumer)
                  (consumer-type-enabled? (:registered-type consumer))
                  consumer))
     consumer)
  ([consumer consumer-tag]
     (if (and
          (consumer-type-enabled? (:registered-type consumer))
          (not (:all-stop @(:conn consumer))))
       (send-off consumer-restart-agent agent-start-consumer! consumer consumer-tag))
     consumer))

(defonce consumer-type-registry (atom {}))

(defn register-consumer [type amqp-credentials handler-functions]
  (swap! consumer-type-registry
         assoc
         type
         {:registered-type   type
          :amqp-credentials  amqp-credentials
          :handler-functions handler-functions}))

(defn unregister-consumer [type]
  (swap! consumer-type-registry
         dissoc
         type))

(defn lookup-conumer [type]
  (let [config (type @consumer-type-registry)]
    (if-not config
      (raise "Error: unregistered consumer type: %s" type))
    config))


(defn add-consumer [type]
  (let [config        (lookup-conumer type)
        conn          (atom (:amqp-credentials  config))
        consumer      (make-consumer type conn (:handler-functions config))]
    (start-consumer! consumer)))

(defn stop-consumer-with-tag
  ([type consumer-tag]
     (log/infof "stop-consumer-with-tag type=%s consumer-tag=%s" type consumer-tag)
     (dosync
      (let [consumer (get-in @active-consumers [type consumer-tag])]
        (log/infof "start-consumer-wtih-tag: consumer=%s" consumer)
        (shutdown-consumer-quietly! consumer)
        (swap! active-consumers
               update-in
               [type]
               dissoc
               consumer-tag))))
  ([type consumer-tag all-stop]
     (dosync
      (let [consumer (get-in @active-consumers [type consumer-tag])]
        (swap! (:conn consumer) assoc :all-stop all-stop)))
     (stop-consumer-with-tag type consumer-tag)))

(defn stop-one [type]
  (dosync
   (let [consumers (type @active-consumers)
         tag1      (ffirst consumers)]
     (if-not (nil? tag1)
       (stop-consumer-with-tag type tag1 true))))
  (count (type @active-consumers)))

(defn stop-all
  ([]
     (doseq [type (keys @active-consumers)]
       (stop-all type)))
  ([type]
     (loop [res (stop-one type)]
       (if (pos? res)
         (recur (stop-one type))))))


