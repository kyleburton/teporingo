(ns teporingo.rpc
  (:require
   [clj-etl-utils.log                                 :as log]
   [clojure.data.json                                 :as json]
   [teporingo.publish                                 :as mq-pub])
  (:use
   [teporingo.core           :only [publish]]
   [clj-etl-utils.lang-utils :only [raise]]))


(defn reply-to-message [message result & [other-data]]
  ;; (mq-pub/lookup (keyword (get-in (first *x*) [:return-to :publisher])))
  ;; (teporingo.broker/find-by-roles (:broker-roles (:sms-publisher @teporingo.publish/*publisher-registry*)))
  (if (contains? message :return-to)
    (let [return-payload (get-in message [:return-to])
          publisher      (keyword (get-in message [:return-to :publisher]))
          return-command (get-in message [:return-to :return-command])
          reply-message  (assoc return-payload
                           :result    result
                           :exception (when (and other-data (contains? other-data :exception))
                                        (:exception other-data)))
          reply-message  (json/json-str {:command return-command :payload reply-message})]

      (mq-pub/with-publisher publisher
        (log/infof "replying with result: msg:%s res:%s publisher:%s" message result publisher)
        (publish reply-message)))
    (log/infof "No return-to for message: %s, discarding %s" message result)))


