(ns teporingo.agent
  (:import
   [java.util.concurrent
    ArrayBlockingQueue
    TimeUnit])
  (:use
   [clj-etl-utils.lang-utils :only [raise]]))

;; use a BlockingQueue to coordinate activity between the publisher
;; and the re-connection agent


(def connection-agent (agent nil))

(defn agent-open-connection! [agent-state conn]
  (swap!
   conn
   assoc
   :conn :the-connection)
  (.offer (:notifier-queue @conn)
          :ok)
  agent-state)

(defn open-connection! [conn]
  (send-off connection-agent agent-open-connection! conn))


(def some-connection
     (atom
      {:open? false
       :notifier-queue (ArrayBlockingQueue. 1)
       :conn nil}))

;; (open-connection! some-connection)
