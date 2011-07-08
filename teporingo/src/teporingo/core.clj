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
          (.confirmSelect channel))
        (when (:use-transactions @conn)
          (.txSelect channel))
        (swap! conn assoc
               :factory factory
               :connection connection
               :channel    channel))))
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

(defn queue-bind! [conn & [queue-name exchange-name routing-key]]
  (if (contains? @conn :connections)
    (doseq [conn (:connections @conn)]
      (queue-bind! conn queue-name exchange-name routing-key))
    (.queueBind
     (:channel          @conn)
     (:queue-name       @conn (or queue-name    ""))
     (:exchange-name    @conn (or exchange-name ""))
     (:routing-key      @conn (or routing-key   *default-routing-key*)))))

(defn make-return-listener [conn handlers]
  (let [handle-return-fn (:handle-return handlers)
        return-listener
        (proxy
            [ReturnListener]
            []
          (handleReturn [reply-code reply-text exchange routing-key props body]
                        (handle-return-fn conn this reply-code reply-text exchange routing-key props body)))]
    {:conn     conn
     :type     :return-listener
     :listener return-listener}))


(defn make-confirm-listener [conn handlers]
  (let [ack-fn  (:handle-ack handlers)
        nack-fn (:handle-nack handlers)
        confirm-listener
        (proxy
            [ConfirmListener]
            []
          (handleAck [delivery-tag multiple]
                     (ack-fn conn this delivery-tag multiple))
          (handleNack [delivery-tag multiple]
                      (nack-fn conn this delivery-tag multiple)))]
    {:conn     conn
     :type     :confirm-listener
     :listener confirm-listener}))


(defn attach-listener! [conn listener]
  (let [channel       (:channel  @conn)
        listener-type (:type     listener)
        listener      (:listener listener)]
    ;;(log/debugf "attaching listener: %s/%s to %s" listener-type listener @conn)
    (cond
      (= :consumer listener-type)
      (do
        (.basicConsume channel
                       (:queue-name   @conn)
                       (:auto-ack     @conn false)
                       (:consumer-tag @conn "")
                       listener))
      (= :return-listener listener-type)
      (.setReturnListener channel  listener)
      (= :confirm-listener listener-type)
      (.setConfirmListener channel listener)
      (= :default-consumer listener-type)
      (.setDefaultConsumer channel listener)
      (= :flow-listener     listener-type)
      (.setFlowListener    channel listener)
      :else
      (raise "Error: unrecognized listener type: %s (not one of: :consumer or :return-listener) in conn=%s listener=%s" (str listener-type)
             @conn
             listener))))


(defn make-default-return-listener [conn]
  (make-return-listener
   conn
   {:handle-return
    (fn [conn listener reply-code reply-text exchange routing-key props body]
      (let [msg (format "RETURNED: conn=%s code=%s text=%s exchange=%s routing-key:%s props=%s body=%s"
                        @conn
                        reply-code
                        reply-text
                        exchange
                        routing-key
                        props
                        (String. body))]
        (log/errorf msg)))}))


(defn delay-by* [ms f]
  (doto (Thread.
         (fn thread-wrapper []
            (Thread/sleep ms)
            (f)))
    (.start)))

(defmacro delay-by [ms & body]
  `(delay-by* ~ms (fn the-delayed [] ~@body)))

