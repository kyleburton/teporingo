(ns teporingo.client
  (:import
   [com.rabbitmq.client
    Consumer
    Envelope
    AMQP$BasicProperties
    ShutdownSignalException]
   [redis.clients.jedis
    Jedis])
  (:require
   [clj-etl-utils.log :as log])
  (:use
   teporingo.core
   [clj-etl-utils.lang-utils :only [raise]]))

(defonce *disabled-consumers-by-type* (atom #{}))

(defn disable-consumer-type [type]
  (swap! *disabled-consumers-by-type* conj type))

(defn enable-consumer-type [type]
  (swap! *disabled-consumers-by-type* disj type))

(defn consumer-type-enabled? [type]
  (not (contains? @*disabled-consumers-by-type* type)))

(defn ack-message []
  (.basicAck (:channel        @*conn*)
             (.getDeliveryTag *envelope*) ;; delivery tag
             false))

(defonce consumer-restart-agent (agent {}))
(declare start-consumer!)
(defonce active-consumers (atom {}))

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
                         (let [raw-body          body
                               [message-id message-timestamp body] (split-body-and-msg-id (String. raw-body))]
                           (binding [*conn*         conn
                                     *consumer*     this
                                     *consumer-tag* consumer-tag
                                     *envelope*     envelope
                                     *properties*   properties
                                     *body*         body
                                     *raw-body*     raw-body
                                     *message-id*   message-id
                                     *message-timestamp* (Long/parseLong message-timestamp)]
                             (delivery)
                             (if (:ack? @conn)
                               (ack-message)))))
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

(declare stop-consumer-with-tag)

(defn agent-start-consumer!
  ([state consumer]
     (try
      (log/debugf "agent-start-consumer! type=%s" (:registered-type consumer))
      (shutdown-consumer-quietly! consumer)
      (log/debugf "agent-start-consumer! ensured shut down, about to start.  Type=%s" (:registered-type consumer))
      (let [conn               (:conn           consumer)]
        (ensure-connection! conn)
        (exchange-declare!  conn)
        (queue-declare!     conn)
        (queue-bind!        conn)
        (attach-listener!   conn consumer))
      (log/debugf "agent-start-consumer! swapping active-consumers for type=%s" (:registered-type consumer))
      (catch Exception ex
        (let [conn               (:conn           consumer)
              reconnect-delay-ms (:reconnect-delay-ms @conn 250)]
          (log/infof ex "agent-start-consumer! error during connect: %s, will re-attempt in %sms" ex reconnect-delay-ms)
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


(comment
  (def *jedis*
       {:host "localhost"
        :port 6379
        :jedis  (atom nil)})

  (defn ensure-jedis-connection [conn]
    (if-not @(:jedis conn)
      (reset! (:jedis conn)
              (Jedis. (:host conn)
                      (:port conn)))))

  (ensure-jedis-connection *jedis*)

  (.set @(:jedis *jedis*) "foo" "bar")
  (.get @(:jedis *jedis*) "foo")
  (.del @(:jedis *jedis*) (into-array String ["foo"]))

  ;; see: http://redis.io/commands/setnx
  (.getSet *jedis* "foo" "first")

  (.setnx *jedis* "foo" "second")

  ;; 1. if the mesage's timestamp is > max-time, route to the expired
  ;; messages queue for investigation.  This can happen when a message
  ;; is delivered _after_ our message-id store expiration or reaping
  ;; (cleanup) time.  This will likely be becuase of a failed rabbit
  ;; not coming back on-line and delivering messages from a very old
  ;; persistent store

  ;; 2. if the message id is marked as 'processed', toss it out
  ;; 3. call (.setnx teporingo.lock.<<msg-id>> <<unix-timestamp>>+<<timemout>>+1) if we get a 1 we got the lock
  ;;     if we got a 0, we did not
  ;; 4. call .get on the lock, check the timeout
  ;; 5. if timed-out, call .getSet with a new tstamp
  ;;     if we get back the earlier lock, then we have the current
  ;;     lock, if not, someone else got the lock
  ;; 6. if we didn't get the lock, sleep (educated, based on the observed timestamps) and go back to the beginning
  ;;

  (defn message-id-processed? [msg-id]
    (let [res (.get @(:jedis *jedis*) (str "teporingo.msg-complete." msg-id))]
      (and
       res
       (>= (Long/parseLong res) 1))))

  (defn set-message-procesed! [msg-id]
    (.incr @(:jedis *jedis*) (str "teporingo.msg-complete." msg-id)))

  (message-id-processed? "foo")
  (set-message-procesed! "foo")

  (with-msg-id-lock msg-id timeout
    body)
  )