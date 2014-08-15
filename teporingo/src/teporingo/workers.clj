(ns teporingo.workers
  (:require
   [clojure.tools.logging    :as log]
   [clj-etl-utils.lang-utils :refer [raise]]))

(defonce registry (atom {}))


(defn- get-worker-config [worker-name]
  (let [worker-config (get @registry worker-name)]
    (if (empty? worker-config)
      (raise "Error: starting unregistered worker %s - %s" worker-name @registry)
      worker-config)))

(defn stop-workers [worker-name]
  (let [worker-config (get-worker-config worker-name)]
    (doseq [t (:threads worker-config) ]
      (reset! (:stop t) true))
    (swap! registry assoc-in [worker-name :threads] [])))

(defn register [worker-name {:keys [worker-fn exception-fn ] :as config} ]
  (when (get @registry worker-name)
    (stop-workers worker-name))
  (swap! registry assoc worker-name (merge config {:threads []})))

(defn start-workers [worker-name number]
  (let [worker-config (get-worker-config worker-name)]
    (dotimes [_ number]
      (let [stop-atom (atom false)
            t         (Thread.
                       (fn []
                         (loop [stop @stop-atom]
                           (when-not stop
                             (try
                              ((:worker-fn worker-config))
                              (catch Exception ex
                                (log/fatalf ex "[%d] Error: worker(%s) encountered an exception" (.getId (Thread/currentThread)) worker-name)))
                             (recur @stop-atom)))))]
        (swap! registry update-in [worker-name :threads] conj {:t t :stop stop-atom
                                                               :thread-id (.getId t)})
        (.start t)))))
