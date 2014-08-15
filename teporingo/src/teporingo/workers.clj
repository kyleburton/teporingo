(ns teporingo.workers
  (:require
   [clojure.tools.logging    :as log]
   [clj-etl-utils.lang-utils :refer [raise]]))

(def registry (atom {}))

(defn register [worker-name {:keys [worker-fn exception-fn ] :as config} ]
  (swap! registry assoc worker-name (merge config {:threads []})))

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

(defn start-workers [worker-name number]
  (let [worker-config (get-worker-config worker-name)]
    (dotimes [_ number]
      (let [stop-atom (atom false)
            t (Thread.
               (fn []
                 (try
                  (loop []
                    ((:worker-fn worker-config))
                    (if-not @stop-atom
                      (recur)))
                  (log/infof "worker(%s) has been instructed exit" worker-name)
                  (catch Exception ex
                    (log/fatalf ex "Error: worker(%s) encountered an exception" worker-name)
                    (when (:exception-fn worker-config)
                      ((:exception-fn worker-config)))))))]
        (swap! registry update-in [worker-name :threads]  conj  {:t t :stop stop-atom})
        (.start t)))))
