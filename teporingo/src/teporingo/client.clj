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

(defonce consumer-restart-agent (agent {}))
(declare start-consumer!)

;; TODO: throw if they didn't provide a :delivery handler?
(defn make-consumer [type conn handlers]
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
                           (shutdown)
                           (log/infof "Consumer[%s/%s] was shut down %s" type consumer-tag sig)
                           (when (:restart-on-connection-closed? @conn)
                             (log/infof "Consumer[%s/%s] will be restarted: conn=%s consumer=%s" type consumer-tag @conn @the-consumer)
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
     (raise "Error: don't know how to shutdown consumer of type=%s, only :consumer is supported. in %s" (:type consumer) consumer))
   (finally
    (close-connection! (:conn consumer))))
  consumer)

(defn shutdown-consumer-quietly! [consumer]
  (try
   (shutdown-consumer! consumer)
   (catch Exception ex
     (log/debugf ex "[IGNORE] Error shutting down consumer: %s" ex)))
  consumer)

(defonce active-consumers (atom {}))

(declare stop-consumer-with-tag)

(def *retry-time* 250)

(defn agent-start-consumer!
  ([state consumer]
     (try
      (log/debugf "agent-start-consumer! type=%s" (:registered-type consumer))
      (shutdown-consumer-quietly! consumer)
      (log/debugf "agent-start-consumer! ensured shut down, about to start.  Type=%s" (:registered-type consumer))
      (let [conn (:conn           consumer)]
        (ensure-connection! conn)
        (exchange-declare!  conn)
        (queue-declare!     conn)
        (queue-bind!        conn)
        (attach-listener!   conn consumer))
      (log/debugf "agent-start-consumer! swapping active-consumers for type=%s" (:registered-type consumer))
      (catch Exception ex

        (log/infof ex "agent-start-consumer! error during connect: %s, will re-attempt in %sms" ex *retry-time*)
        (delay-by
         *retry-time*
         (send-off consumer-restart-agent agent-start-consumer! consumer))))
     state)
  ([state consumer consumer-tag]
     (log/debugf "agent-start-consumer! type:%s tag:%s" (:registered-type consumer) consumer-tag)
     (stop-consumer-with-tag (:registered-type consumer) consumer-tag)
     (agent-start-consumer! state consumer)))

(defn start-consumer!
  ([consumer]
     (send-off consumer-restart-agent agent-start-consumer! consumer)
     consumer)
  ([consumer consumer-tag]
     (send-off consumer-restart-agent agent-start-consumer! consumer consumer-tag)
     consumer))

(defonce consumer-type-registry (atom {}))

(defn register-consumer [type amqp-credentials handler-functions]
  (swap! consumer-type-registry
         assoc
         type
         {:registered-type   type
          :amqp-credentials  amqp-credentials
          :handler-functions handler-functions}))

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

(defn stop-consumer-with-tag [type consumer-tag]
  (log/infof "stop-consumer-with-tag type=%s consumer-tag=%s" type consumer-tag)
  (dosync
   (let [consumer (get-in @active-consumers [type consumer-tag])]
     (log/infof "consumer=%s" consumer)
     (.println System/err (format "consumer=%s" consumer))
     (swap! (:conn consumer) assoc :restart-on-connection-closed? false)
     (shutdown-consumer-quietly! consumer)
     (swap! active-consumers
            update-in
            [type]
            dissoc
            consumer-tag))))

(defn stop-one [type]
  (dosync
   (let [consumers (type @active-consumers)
         tag1      (ffirst consumers)]
     (if-not (nil? tag1)
      (stop-consumer-with-tag type tag1))))
  (count (type @active-consumers)))

(defn stop-all [type]
  (loop [res (stop-one type)]
    (if (pos? res)
      (recur (stop-one type)))))

(comment
  (register-consumer
   :foof
   {:port                          25672
    :vhost                         "/"
    :exchange-name                 "/foof"
    :queue-name                    "foofq"
    :heartbeat-seconds             5
    :restart-on-connection-closed? true
    :ack?                          true}
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

  (let [ex (agent-error consumer-restart-agent)]
    (log/infof ex "Error: %s" ex))

  (clear-agent-errors consumer-restart-agent)

  )
