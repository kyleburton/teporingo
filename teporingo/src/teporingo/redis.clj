(ns teporingo.redis
  (:import
   [redis.clients.jedis
    Jedis JedisPool JedisPoolConfig])
  (:use
   [clj-etl-utils.lang-utils :only [raise]]))

(def *jedis-pools* (atom {}))

(defn register-redis-pool
  ([name]
     (register-redis-pool name {:host "localhost" :port 6379}))
  ([name configuration]
   (swap! *jedis-pools* assoc name
          {:name          name
           :configuration configuration
           :pool          (JedisPool.
                           (JedisPoolConfig.)
                           (:host configuration)
                           (:port configuration))})))


(defn get-redis-pool [name]
  (get @*jedis-pools* name))

(def ^{:dynamic true} *jedis* :does-not-exist)

(defn with-jedis* [name the-fn]
  (let [instance (atom nil)]
    (try
     (reset! instance (.getResource (:pool (get-redis-pool name))))
     (binding [*jedis* @instance]
      (the-fn))
     (finally
      (if-not (nil? @instance)
       (.returnResource instance))))))



(comment
  (register-redis-pool :localhost)
  (defn ensure-jedis-connection [conn]
    (if-not @(:jedis conn)
      (reset! (:jedis conn)
              (JedisPool.
               (JedisPoolConfig.)
               (:host conn)
               (:port conn)))))

  (ensure-jedis-connection *jedis*)

  (.set @(:jedis *jedis*) "foo" "bar")
  (.get @(:jedis *jedis*) "foo")
  (.del @(:jedis *jedis*) (into-array String ["foo"]))

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