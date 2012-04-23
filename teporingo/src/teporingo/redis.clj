(ns teporingo.redis
  (:import
   [redis.clients.jedis
    Jedis JedisPool JedisPoolConfig Protocol])
  (require
   [clj-etl-utils.log :as log])
  (:use
   teporingo.redis.bindings
   [teporingo.core :only [*message-id* *consumer-tag*]]
   [clj-etl-utils.lang-utils :only [raise aprog1]]))

(defonce *jedis-pools* (atom {}))

(defn register-redis-pool
  ([name]
     (register-redis-pool name {:host "localhost" :port 6379}))
  ([name configuration]
     (swap! *jedis-pools* assoc name
            {:name          name
             :configuration configuration
             :pool          (JedisPool.
                             (JedisPoolConfig.)
                             (:host     configuration "localhost")
                             (:port     configuration Protocol/DEFAULT_PORT)
                             (:timeout  configuration Protocol/DEFAULT_TIMEOUT)
                             (:password configuration nil))})))

(defn get-redis-pool [name]
  (get @*jedis-pools* name))

(defn with-jedis* [name the-fn]
  (let [instance (atom nil)
        pool     (:pool (get-redis-pool name))]
    (try
     (reset! instance (.getResource pool))
     (binding [*jedis* @instance]
       (the-fn))
     (finally
      (if-not (nil? @instance)
        (.returnResource pool @instance))))))

(defmacro with-jedis [name & body]
  `(with-jedis* ~name (fn [] ~@body)))

(def *lock-timeout* (* 10 1000))

;; TODO: move these into teporingo.redis.concurrent
(defn- perform-processing-and-release-lock [the-fn now tstamp lock-key process-key]
  (aprog1
      ;; NB: added in try/catch exception handling - release the lock in the finally block
      {:res    :ok
       :reason :processed
       :val    (the-fn)}
    (log/infof "[%s] adding processed key [%s@%s] and releasing lock %s@%s"
               *consumer-tag*
               process-key tstamp lock-key tstamp)
    (.zadd *jedis* "teporingo.keys.processed" (double now) process-key)
    (.del *jedis* (into-array String [lock-key]))))

(defn- acquire-lock? [lock-key tstamp]
  (= 1 (.setnx *jedis* lock-key tstamp)))

(defn- already-processed? [process-key]
  (.zscore *jedis* "teporingo.keys.processed" process-key))

(defn with-jedis-dedupe* [spec the-fn]
  (let [the-key     (:key spec)
        process-key (str "tep." the-key ".processed")
        lock-key    (str "tep." the-key ".lock")
        now         (.getTimeInMillis (java.util.Calendar/getInstance))
        timeout     (:timeout spec *lock-timeout*)
        tstamp      (str (+ now timeout 1000))
        retries     (:retries spec 0)
        max-retries (:max-retries spec 3)]
    (log/infof "[%s] enter lock guard: %s@%s" *consumer-tag* the-key tstamp)
    (if (> retries max-retries)
      {:res         :failure
       :reason      :max-retries
       :max-retries max-retries
       :retries     retries}
      (if (already-processed? process-key)
        (do
          (log/infof "[%s] key already processed: %s" *consumer-tag* process-key)
          {:res    :ok
           :reason :duplicate})
        (if (acquire-lock? lock-key tstamp)
          ;; have lock, call the fn and release the lock
          (do
            (log/infof "[%s] got the lock[%s@%s], processing" *consumer-tag* lock-key tstamp)
            (perform-processing-and-release-lock the-fn now tstamp lock-key process-key))
          ;; do not have the lock, see if the old lock expired
          (let [other-tstamp   (.get *jedis* lock-key)
                other-exp-time (Long/parseLong other-tstamp)
                expired?       (> now other-exp-time)]
            (log/infof "[%s] checking if other lock[%s] is expired now:%s vs lock-tstamp:%s => %s"
                       *consumer-tag*
                       lock-key now other-tstamp expired?)
            (if expired?
              ;; they timedout, try to get the lock
              (if (= other-tstamp (.getSet *jedis* lock-key tstamp))
                (if (already-processed? process-key)
                  (do
                    (log/infof "[%s] got lock, but was already processed by the time we stole the lock." *consumer-tag*)
                    (.del *jedis* (into-array String [lock-key]))
                    {:res    :ok
                     :reason :duplicate
                     :note   :after-stealing-lock})
                  (do
                    (log/infof "[%s] acquired the expired lock %s@%s" *consumer-tag* lock-key tstamp)
                    (perform-processing-and-release-lock the-fn now tstamp lock-key process-key)))
                ;; someone else got the lock, sleep and retry
                (let [sleep-time (- other-exp-time now)]
                  (log/infof "[%s] could not steal lock[%s], will retry after:%s" *consumer-tag* lock-key sleep-time)
                  (Thread/sleep 1000)
                  (recur spec the-fn)))
              ;; they still hold the lock, sleep and retry
              (let [sleep-time (- other-exp-time now)]
                (log/infof "[%s] another process holds the lock[%s], will retry after:%s" *consumer-tag* lock-key sleep-time)
                (Thread/sleep 1000)
                (recur spec the-fn)))))))))

(defmacro with-jedis-dedupe [spec & body]
  `(with-jedis-dedupe* ~spec (fn [] ~@body)))

(defn make-deduping-delivery-fn [spec key-namespace handler-fn]
  (fn []
    (with-jedis (:redis-instance spec)
      (with-jedis-dedupe (assoc spec :key (str key-namespace "." *message-id*))
        (handler-fn)))))

(comment
  (register-redis-pool :localhost)

  (let [spec {:key "foo" :timeout 5000}]
   (with-jedis :localhost
     (with-jedis-dedupe spec
       (log/infof "we got the foo lock!")
       (Thread/sleep 10000)
       (.incr *jedis* (:key spec))
       (log/infof "finished processing"))))

  (with-jedis :localhost
    (.get *jedis* "foo"))

  (with-jedis :localhost
    (.zadd *jedis* "foo" (double 234) "1234"))

  (with-jedis :localhost
    (.zscore *jedis* "teporingo.keys.processed" "foo"))

  (with-jedis :localhost
    (.zrange *jedis* "teporingo.keys.processed" 0 -1))

  (with-jedis :localhost
    (.zscore *jedis* "teporingo.keys.processed" "foo.processed"))

  (with-jedis :localhost
    (.get *jedis* "teporingo.keys.processed"))

  (with-jedis :localhost
    (.del *jedis* (into-array String ["foo"])))

  (with-jedis :localhost
    (.del *jedis*
          (into-array String
                      (vec (with-jedis :localhost
                             (.keys *jedis* "*"))))))

  ;; see: http://redis.io/commands/setnx
  (.getSet *jedis* "foo" "first")

  (.setnx *jedis* "foo" "second")

  ;; 1. if the mesage's timestamp is > max-time, route to the expired
  ;; messages queue for investigation.  This can happen when a message
  ;; is delivered _after_ our message-id store expiration or reaping
  ;; (cleanup) time.  This will likely be becuase of a failed rabbit
  ;; not coming back on-line and delivering messages from a very old
  ;; persistent store

  ;; 2. if the message id is marked as 'processed', toss it out
  ;; 3. call (.setnx teporingo.lock.<<msg-id>> <<unix-timestamp>>+<<timemout>>+1)
  ;;     if we get a 1 we got the lock
  ;;     if we got a 0, we did not
  ;; 4. call .get on the lock, check the timeout
  ;; 5. if timed-out, call .getSet with a new tstamp
  ;;     if we get back the earlier lock, then we have the current
  ;;     lock, if not, someone else got the lock
  ;; 6. if we didn't get the lock, sleep (educated, based on the observed timestamps)
  ;;     and go back to the beginning
  ;;

  (defn message-id-processed? [msg-id]
    (let [res (.get @(:jedis *jedis*) (str "teporingo.msg-complete." msg-id))]
      (and
       res
       (>= (Long/parseLong res) 1))))

  (defn set-message-procesed! [msg-id]
    (.incr @(:jedis *jedis*) (str "teporingo.msg-complete." msg-id)))

  (message-id-processed? "foo")
  (set-message-procesed! "foo")

  (with-msg-id-lock msg-id timeout
    body)
  )
