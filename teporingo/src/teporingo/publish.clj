(ns teporingo.publish
  (:import
   [com.rabbitmq.client
    ConnectionFactory
    Connection
    Channel
    AlreadyClosedException
    MessageProperties
    Envelope
    AMQP$BasicProperties
    ShutdownSignalException]
   [java.io IOException]
   ;; [java.util.concurrent ArrayBlockingQueue]
   [com.github.kyleburton.teporingo BreakerOpenException MaxPublishRetriesExceededException])
  (:require
   [clj-etl-utils.log :as log]
   [teporingo.breaker :as breaker])
  (:use
   teporingo.core
   [clj-etl-utils.lang-utils :only [raise]]))

(defonce *broker-registry* (atom {}))

(defn register-amqp-broker-cluster [name config]
  (swap! *broker-registry* assoc name config))

(defn unregister-amqp-broker-cluster [name]
  (swap! *broker-registry* dissoc name))

(defn ensure-publisher [conn]
  (if (contains? conn :connections)
    (doseq [conn (:connections conn)]
      (ensure-publisher conn))
    (when (nil? (:channel conn))
      (ensure-connection! conn)
      (exchange-declare!  conn)
      (queue-declare!     conn)
      (queue-bind!        conn)
      (swap! conn
             assoc
             :message-acks
             (java.util.concurrent.ConcurrentHashMap.))
      {:res true :ex nil}))
  conn)

(defn make-publish-circuit-breaker [opts]
  (breaker/basic-breaker
   (fn [conn exchange routing-key mandatory immediate props body]
     (try
      (ensure-publisher conn)
      (let [channel (:channel @conn)]
        (if channel
          (do
            (.basicPublish
             channel
             exchange
             routing-key
             mandatory
             immediate
             props
             body)
            (when (:use-transactions conn)
              (log/infof "calling .txCommit on channel")
              (.txCommit channel))
            {:res true :ex nil})
          {:res false :ex nil}))
      (catch AlreadyClosedException ex
        (log/errorf ex "Error publishing to %s: %s" @conn ex)
        (close-connection! conn)
        (throw ex))
      (catch IOException ex
        (log/errorf ex "Error publishing to %s: %s" @conn ex)
        (close-connection! conn)
        (throw ex))))
   opts))


;; NB: for performance, publish-1's use of ensure-publisher will have
;; to implement a circuit breaker - the timeout on establishing a
;; connection takes way too long for this to be a viable approach -
;; it'll end up creating too much back pressure in the event we've got
;; 1 or more brokers down.

;; NB: how do we handle when the message goes no where?
;; it's not a confirmListner, b/c in this event the message is returned
;; when it's returned it doesn't have a sequenceNo on the message, so
;; there is no way to coorelate the message we attempted to publish
;; with the one that was returned
(defn publish-1 [^Atom conn ^String exchange ^String routing-key ^Boolean mandatory ^Boolean immediate ^AMQP$BasicProperties props ^bytes body]
  (try
   ((:publish @conn) conn exchange routing-key mandatory immediate props body)
   {:res true :ex nil}
   (catch IOException ex
     (log/errorf ex "Error: conn[%s] initilizing the publisher: %s" @conn ex)
     (close-connection! conn)
     {:res false :ex ex})
   (catch BreakerOpenException ex
     (log/errorf ex "Error: conn[%s] circuit breaker is open: %s" @conn ex)
     {:res false :ex ex})))

(defonce breaker-agent (agent {}))

(defn breaker-agent-open-connection [state conn]
  (try
   (close-connection!  conn)
   (ensure-connection! conn)
   (exchange-declare!  conn)
   (queue-declare!     conn)
   (queue-bind!        conn)
   (swap! conn assoc :closed? false)
   #_(.offer (:connection-statusq @conn) :ok)
   (catch Exception ex
     (log/warnf ex "Error re-establishing connection, will retry: %s %s" ex @conn)
     (.start
      (Thread.
       ;; TODO: make this a daemon thread, and allow the infinite restart loop to be stopped / broken out of...
       (fn []
         (Thread/sleep (:reconnect-delay-ms @conn 250))
         (log/warnf "Delayed reconnect, re-sending off after error connecting previous time...")
         (send-off breaker-agent breaker-agent-open-connection conn))))))
  state)

(defn make-pub-agent-breaker [conn]
  #_(if-not (:connection-statusq @conn)
      (swap! conn assoc :connection-statusq (ArrayBlockingQueue.)))
  (breaker/make-circuit-breaker
   (fn inner-fn [the-conn exchange routing-key mandatory immediate props body]
     (do
       (let [channel (:channel @conn)]
         (.basicPublish
          channel
          exchange
          routing-key
          mandatory
          immediate
          props
          body)
         (when (:use-transactions conn)
           (log/infof "calling .txCommit on channel")
           (.txCommit channel)))
       {:res true :ex nil}))
   (fn closed-fn? [state]
     (:closed? @conn))
   (fn err-fn [state ex]
     ;; (swap! conn update-in [:errors] conj ex)
     (swap! conn assoc :closed? true)
     (send-off breaker-agent breaker-agent-open-connection conn))))


(defn publish [^Map publisher ^String exchange ^String routing-key ^Boolean mandatory ^Boolean immediate ^AMQP$BasicProperties props ^bytes body retries & [errors]]
  (when (< retries 1)
    (log/errorf "Error: exceeded max retries for publish %s : %s" publisher
                (vec errors))
    (doseq [err errors]
      (if err
        (log/errorf err "Max retries due to: %s" err)))
    (raise (MaxPublishRetriesExceededException. "Error: exceeded max retries for publish." (first errors) (into-array Throwable errors))))
  ;; try publishing to all brokers, ensure we publish to at least the min required
  (let [num-published             (atom 0)
        min-brokers-published-to  (:min-brokers-published-to publisher 1)
        pub-errs                  (atom [])
        mandatory                 (if-not (nil? mandatory) mandatory true)
        immediate                 (if-not (nil? immediate) immediate true)
        message-props             (or props MessageProperties/PERSISTENT_TEXT_PLAIN)]
    (log/infof "publish: mandatory:%s immediate:%s" mandatory immediate)
    (doseq [conn (:connections publisher)]
      (let [res (publish-1 conn exchange routing-key mandatory immediate props body)]
        (if (:res res)
          (swap! num-published inc)
          (swap! pub-errs conj (:ex res)))))
    (if (< @num-published min-brokers-published-to)
      (do
        (log/debugf "num-published %s was <%s, retrying..." @num-published min-brokers-published-to)
        (publish publisher exchange routing-key mandatory immediate props body (dec retries) (concat errors @pub-errs)))
      (log/debugf "looks like we published to %s brokers.\n" @num-published))))

(defn make-publisher [name]
  (let [config    (get @*broker-registry* name)
        publisher {:connections (vec (map (fn [m] (atom m)) config))}]
    (doseq [conn (:connections publisher)]
      #_(swap! conn
             assoc
             :connection-statusq (ArrayBlockingQueue.))
      (swap! conn
             assoc
             :errors []
             :publish
             (make-pub-agent-breaker conn))
      (send breaker-agent breaker-agent-open-connection conn))
    publisher))

