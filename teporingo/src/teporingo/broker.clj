(ns teporingo.broker
  (:use
   [clojure.set :only [intersection]]
   [clj-etl-utils.lang-utils :only [raise]]))


(defonce *broker-registry* (atom {}))

(defonce *disabled-brokers* (atom #{}))

(defn disable [registered-name]
  (swap! *disabled-brokers* conj registered-name))

(defn enable [registered-name]
  (swap! *disabled-brokers* disj registered-name))

(defn enabled? [registered-name]
  (not (contains? @*disabled-brokers* registered-name)))

(defn lookup [name]
  (get @*broker-registry* name))

(defn register [name config]
  (swap! *broker-registry* assoc name config))

(defn unregister [name]
  (swap! *broker-registry* dissoc name))

(defn find-by-roles [#^java.util.Set tags]
  (vec
   (filter
    (fn [config]
      (not (empty? (intersection tags (:roles config)))))
    (vals @*broker-registry*))))

;; (find-by-roles #{:local})

