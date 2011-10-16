(ns teporingo.breaker
  (:import
   [com.github.kyleburton.teporingo
    BreakerOpenException
    BreakerOpenedException
    BreakerReOpenedException])
  (:require
   [clj-etl-utils.log :as log])

  (:use
   [clj-etl-utils.lang-utils :only [raise aprog1]]))

(defn basic-breaker
  ([inner-fn]
     (basic-breaker inner-fn {:retry-after 2}))
  ([inner-fn opts]
     (let [state (atom (merge {:closed? true
                               :retry-after 2
                               :current-retries 0}
                              opts))]
       (fn [& args]
         (cond
           (:closed? @state)
           (try
            (apply inner-fn args)
            (catch Exception ex
              (log/errorf ex (str ex))
              (swap! state
                     assoc
                     :closed? false
                     :current-retries 0)
              (throw (BreakerOpenedException.
                      (format "Breaker[%s] Opened by: %s" @state ex)
                      ex))))
           (>= (:current-retries @state)
               (:retry-after     @state))
           (try
            (aprog1
                (apply inner-fn args)
              (swap!
               state
               assoc
               :closed? true
               :current-retries 0))
            (catch Exception ex
              (log/errorf ex (str ex))
              (swap! state
                     assoc
                     :closed? false
                     :current-retries 0)
              (throw (BreakerReOpenedException.
                      (format "Breaker[%s] Opened during re-attempt by: %s" @state ex)
                      ex))))
           :else
           (do
             (swap!
              state
              update-in
              [:current-retries]
              inc)
             (throw (BreakerOpenException. (format "Breaker[%s] (still) Open." @state)))))))))

(defmacro defbreaker [type fname argspec & body]
  (let [sym (symbol (str (name type) "-breaker"))]
    `(def ~fname
          (~sym
           (fn ~argspec
             ~@body)))))


(defn make-circuit-breaker [inner-fn open-fn? on-err-fn!]
  (let [state (atom {:open-fn?  open-fn?
                     :on-err-fn! on-err-fn!})]
    (fn [& args]
      (cond
        ((:open-fn? @state) state)
        (throw (BreakerOpenException. "Breaker Open"))

        :else
        (try
         (aprog1
             (apply inner-fn args))
         (catch Exception ex
           (on-err-fn! state ex)
           (throw (BreakerOpenedException. (format "Breaker Opened, inner fn failed: %s" ex)
                                           ex))))))))




