(ns teporingo.core
  (:import
   [java.io IOException]
   [com.rabbitmq.client
    ConnectionFactory
    Connection
    Channel
    Consumer
    AlreadyClosedException
    ReturnListener
    ConfirmListener
    FlowListener
    MessageProperties
    Envelope
    AMQP$BasicProperties
    ShutdownSignalException]
   [com.github.kyleburton.teporingo BreakerOpenException])
  (:require
   [clj-etl-utils.log :as log]
   [teporingo.breaker :as breaker]
   [rn.clorine.pool :as pool]
   [clojure.contrib.json :as json])
  (:use
   [clj-etl-utils.lang-utils :only [raise aprog1]]))

;; http://en.wikipedia.org/wiki/Volcano_Rabbit
;;   => teporingo

;; a 'connection' is a map: {:connections [...]}, where each
;; connection is a managed map wrapped in an atom where each map
;; contains the connection information and broker credentails, along
;; with the connection information and credentials

(def *default-routing-key* "#")

;; NB: when we hit clojure 1.3, use ^:dynamic
(def *conn*         nil)
(def *consumer*     nil)
(def *consumer-tag* nil)
(def *envelope*     nil)
(def *properties*   nil)
(def *body*         nil)
(def *sig*          nil)
(def *listener*     nil)
(def *reply-code*   nil)
(def *reply-text*   nil)
(def *exchange*     nil)
(def *routing-key*  nil)
(def *props*        nil)
(def *message-properties* nil)
(def *confirm-type* nil)
(def *delivery-tag* nil)
(def *multiple*     nil)
(def *active*       nil)

(declare make-return-listener)
(declare make-confirm-listener)
(declare make-flow-listener)

(defn attach-listener! [conn listener]
  (let [channel       (:channel  @conn)
        listener-type (:type     listener)
        listener      (:listener listener)]
    (log/infof "attach-listener! attaching[%s] %s to %s"
               listener-type listener @conn)
    (cond
      (= :consumer listener-type)
      (do
        (.basicConsume channel
                       (:queue-name   @conn)
                       (:auto-ack     @conn false)
                       (:consumer-tag @conn "")
                       listener))
      (= :return listener-type)
      (.setReturnListener channel  (:listener (make-return-listener conn listener)))
      (= :confirm listener-type)
      (.setConfirmListener channel (:listener (make-confirm-listener conn listener)))
      ;; NB: allowing a default consumer is questionable IMO
      ;; if we do that, we should wrap this in a (make-default-consumer-listner listener) as we do with the other listener types
      ;; (= :default-consumer listener-type)
      ;; (.setDefaultConsumer channel listener)
      (= :flow listener-type)
      (.setFlowListener    channel (:listener (make-flow-listener conn listener)))
      :else
      (raise "Error: unrecognized listener type: %s (not one of: :consumer or :return-listener) in conn=%s listener=%s" (str listener-type)
             @conn
             listener))))


(defn ensure-connection! [conn]
  (if (contains? conn :connections)
    (doseq [conn (:connections conn)]
      (ensure-connection! conn))
    (when (nil? (:channel @conn))
      (let [factory (aprog1
                        (ConnectionFactory.)
                      (.setConnectionTimeout  it (:connection-timeout @conn 0))
                      (.setUsername           it (:user  @conn "guest"))
                      (.setPassword           it (:pass  @conn "guest"))
                      (.setVirtualHost        it (:vhost @conn "/"))
                      (.setHost               it (:host  @conn "localhost"))
                      (.setPort               it (:port  @conn 5672))
                      (.setRequestedHeartbeat it (:heartbeat-seconds @conn 0)))
            connection (.newConnection factory)
            channel    (.createChannel connection)]
        (when (:use-confirm @conn)
          (log/infof "setting .confirmSelect on connection")
          (.confirmSelect channel))
        (when (:use-transactions @conn)
          (log/infof "setting .txSelect on connection")
          (.txSelect channel))
        (swap! conn assoc
               :factory factory
               :connection connection
               :channel    channel)
        (doseq [listener-type [:flow :return :confirm]]
          (if-let [listener (listener-type (:listeners @conn))]
            (attach-listener! conn {:type listener-type :listener listener}))))))
  conn)



(defn close-quietly [thing]
  (let [result (atom {:close-result nil
                      :exception nil})]
    (try
     (swap! result assoc :close-result (.close thing))
     (catch Exception ex
       (swap! result assoc :exception ex)))
    @result))

(defn close-connection! [conn]
  (if (contains? conn :connections)
    (doseq [conn (:connections conn)]
      (close-connection! conn))
    (do
      (close-quietly (:connection @conn))
      (close-quietly (:channel @conn))
      (swap! conn dissoc :channel :connection :factory)))
  conn)

(defn exchange-declare! [conn & [exchange-name exchange-type exchange-durable]]
  (if (contains? @conn :connections)
    (doseq [conn (:connections @conn)]
      (exchange-declare! conn exchange-name exchange-type exchange-durable))
    (.exchangeDeclare
     (:channel          @conn)
     (:exchange-name    @conn exchange-name)
     (:exchange-type    @conn (or exchange-type "direct"))
     (:exchange-durable @conn (or exchange-durable true)))))

(defn queue-declare! [conn & [name durable exclusive autodelete arguments]]
  (if (contains? @conn :connections)
    (doseq [conn (:connections @conn)]
      (queue-declare! conn name durable exclusive autodelete arguments))
    (.queueDeclare
     (:channel          @conn)
     (:queue-name       @conn name)
     (:queue-durable    @conn (if-not (nil? durable)    durable   true))
     (:queue-exclusive  @conn (if-not (nil? exclusive)  exclusive false))
     (:queue-autodelete @conn (if-not (nil? autodelete) autodelete false))
     (:queue-arguments  @conn (or arguments {})))))

(defn queue-bind! [conn]
  (if (contains? @conn :connections)
    (doseq [conn (:connections @conn)]
      (queue-bind! conn))
    (doseq [binding (:bindings @conn)]
      (let [queue-name    (:queue-name       binding (:queue-name    @conn))
            exchange-name (:exchange-name    binding (:exchange-name @conn))
            routing-key   (:routing-key      binding (:routing-key   @conn ""))]
        (log/infof "binding conn:%s queue-name:%s exchange-name:%s routing-key:%s"
                   @conn
                   queue-name
                   exchange-name
                   routing-key)
        (.queueBind
         (:channel          @conn)
         queue-name
         exchange-name
         routing-key)))))

(defn make-return-listener [conn handle-return-fn]
  (log/infof "make-return-listener: conn=%s" conn)
  (def *foo* conn)
  {:conn     conn
   :type     :return-listener
   :listener
   (proxy
       [ReturnListener]
       []
     (handleReturn
      [reply-code reply-text exchange routing-key props body]
      (binding [*reply-code*         reply-code
                *reply-text*         reply-text
                *exchange*           exchange
                *routing-key*        routing-key
                *message-properties* props
                *listener*           this
                *conn*               conn
                *props*              props
                *body*               body]
        (handle-return-fn))))})


(defn make-confirm-listener [conn handler-fn]
  {:conn     conn
   :type     :confirm-listener
   :listener
   (proxy
       [ConfirmListener]
       []
     (handleAck
      [delivery-tag multiple]
      (binding [*confirm-type* :ack
                *conn*         conn
                *listener*     this
                *delivery-tag* delivery-tag
                *multiple*     multiple]
        (handler-fn)))
     (handleNack
      [delivery-tag multiple]
      (binding [*confirm-type* :nack
                *conn*         conn
                *listener*     this
                *delivery-tag* delivery-tag
                *multiple*     multiple]
        (handler-fn))))})

(defn make-flow-listener [conn handler-fn]
  {:conn     conn
   :type     :flow-listener
   :listener
   (proxy
       [FlowListener]
       []
     (handleFlow
      [active]
      (binding [*conn*         conn
                *listener*     this
                *active*       active]
        (handler-fn))))})



(defn delay-by* [ms f]
  (doto (Thread.
         (fn thread-wrapper []
           (Thread/sleep ms)
           (f)))
    (.start)))

(defmacro delay-by [ms & body]
  `(delay-by* ~ms (fn the-delayed [] ~@body)))

