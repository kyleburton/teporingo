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
     (let [state (atom nil)
           opts (merge {:closed?         true
                        :retry-after     2
                        :current-retries 0
                        :last-error      nil
                        :breaker-opened-fn (fn [state cause]
                                             (throw (BreakerOpenedException.
                                                     (format "Breaker[%s] Opened by: %s" @state cause)
                                                     cause)))
                        :breaker-open-fn (fn [state cause re-openend]
                                           (throw (BreakerOpenException. (format "Breaker[%s] (re-attempt:%s) Open." @state re-openend)
                                                                         (:last-error state))))
                        :breaker-closed-fn (fn [state]
                                             nil)}
                       opts)
           breaker-opened-fn   (:breaker-opened-fn opts)
           breaker-open-fn     (:breaker-open-fn opts)
           breaker-closed-fn   (:breaker-closed-fn opts)]
       (reset! state opts)
       (fn [& args]
         (cond
           ;; normal attempt
           (:closed? @state)
           (try
            (apply inner-fn args)
            (catch Exception ex
              (log/errorf ex (str ex))
              (swap! state
                     assoc
                     :closed? false
                     :current-retries 0
                     :last-error ex)
              (breaker-opened-fn state ex)))
           ;; should we retry?
           (>= (:current-retries @state)
               (:retry-after     @state))
           (try
            (aprog1
                (apply inner-fn args)
              ;; inner-fn did not throw? cool we just closed
              (swap!
               state
               assoc
               :closed? true
               :current-retries 0
               :last-error nil)
              (breaker-closed-fn state))
            (catch Exception ex
              ;; reattempt failed, breaker reopened
              (log/errorf ex (str ex))
              (swap! state
                     assoc
                     :closed? false
                     :current-retries 0
                     :last-error ex)
              (breaker-open-fn state ex true)))
           ;; breaker is open: not ready to retry yet
           :else
           (do
             (swap!
              state
              update-in
              [:current-retries]
              inc)
             (breaker-open-fn state nil false)))))))

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




