(ns teporingo.redis.client
  (:use
   teporingo.redis.bindings
   [clj-etl-utils.lang-utils :only [raise]])
  (:import
   [redis.clients.jedis
    Jedis JedisPool JedisPoolConfig JedisPubSub]))

;; teporingo.redis
(defn ping
  ([]
     (.ping *jedis*))
  ([c]
     (.ping c)))

(defn flush-all
  ([]
     (.flushAll *jedis*))
  ([c]
     (.flushAll c)))

;; Keys

(defn exists
  ([^String k]
     (.exists *jedis* k))
  ([conn ^String k]
     (.exists conn k)))

(defn del [& args]
  (cond
    (isa? (class (first args)) String)
    (.del *jedis* ^"[Ljava.lang.String;" (into-array String args))

    :first-arg-is-connection
    (let [[conn & keys] args]
      (.del conn ^"[Ljava.lang.String;" (into-array String keys)))))

(defn keys
  ([^String pattern]
     (seq (.keys *jedis* (or pattern "*"))))
  ([conn ^String pattern]
     (seq (.keys conn (or pattern "*")))))

(defn rename
  ([^String k ^String nk]
     (.rename *jedis* k nk))
  ([conn ^String k ^String nk]
     (.rename conn k nk)))

(defn renamenx
  ([^String k ^String nk]
     (.renamenx *jedis* k nk))
  ([conn ^String k ^String nk]
     (.renamenx conn k nk)))

(defn expire
  ([^String k ^Number s]
     (.expire *jedis* k (int s)))
  ([conn ^String k ^Number s]
     (.expire conn k (int s))))

(defn expireat
  ([^String k ^Number ut]
     (.expireAt *jedis* k (long ut)))
  ([conn ^String k ^Number ut]
     (.expireAt conn k (long ut))))

(defn ttl
  ([^String k]
     (.ttl *jedis* k))
  ([conn ^String k]
     (.ttl conn k)))

(defn persist
  ([^String k]
     (.persist *jedis* k))
  ([conn ^String k]
     (.persist conn k)))

(defn move
  ([^String k ^Number db]
     (.move *jedis* k (int db)))
  ([conn ^String k ^Number db]
     (.move conn k (int db))))

(defn type
  ([^String k]
     (.type *jedis* k))
  ([conn ^String k]
     (.type conn k)))


;; Strings

(defn incr
  ([^String k]
     (.incr *jedis* k))
  ([conn ^String k]
     (.incr conn k)))

(defn incrby
  ([^String k ^Number v]
     (.incrBy *jedis* k (long v)))
  ([conn ^String k ^Number v]
     (.incrBy conn k (long v))))

(defn decr
  ([^String k]
     (.decr *jedis* k))
  ([conn ^String k]
     (.decr conn k)))

(defn decrby
  ([^String k ^Number v]
     (.decrBy *jedis* k (long v)))
  ([conn ^String k ^Number v]
     (.decrBy conn k (long v))))

(defn get
  ([^String k]
     (.get *jedis* k))
  ([conn ^String k]
     (.get conn k)))

(defn set
  ([^String k ^String v]
     (.set *jedis* k v))
  ([conn ^String k ^String v]
     (.set conn k v)))

(defn mget [& args]
  (cond
    (isa? (class (first args)) String)
    (seq (.mget *jedis* ^"[Ljava.lang.String;" (into-array String args)))

    :first-arg-is-connection
    (let [[conn & keys] args]
      (seq (.mget conn ^"[Ljava.lang.String;" (into-array String keys))))))

(defn mset [& args]
  (cond
    (isa? (class (first args)) String)
    (.mset *jedis* ^"[Ljava.lang.String;" (into-array String args))
    :first-arg-is-connection
    (.mset (first args) ^"[Ljava.lang.String;" (into-array String (rest args)))))

(defn msetnx [& args]
  (cond
    (isa? (class (first args)) String)
    (.msetnx *jedis* ^"[Ljava.lang.String;" (into-array String args))
    :first-arg-is-connection
    (.msetnx (first args) ^"[Ljava.lang.String;" (into-array String (rest args)))))

(defn getset
  ([^String k ^String v]
     (.getSet *jedis* k v))
  ([conn ^String k ^String v]
     (.getSet conn k v)))

(defn append
  ([^String k ^String v]
     (.append *jedis* k v))
  ([conn ^String k ^String v]
     (.append conn k v)))

(defn getrange
  ([^String k ^Number start ^Number end]
     (.getrange *jedis* k (long start) (long end)))
  ([conn ^String k ^Number start ^Number end]
     (.getrange conn k (long start) (long end))))

(defn setnx
  ([^String k ^String v]
     (.setnx *jedis* k v))
  ([conn ^String k ^String v]
     (.setnx conn k v)))

(defn setex
  ([^String k ^Number s ^String v]
     (.setex *jedis* k (int s) v))
  ([conn ^String k ^Number s ^String v]
     (.setex conn k (int s) v)))


;; Lists

(defn lpush [& args]
  (cond
    (isa? (class (first args)) String)
    (let [[k & vals] args]
      (.lpush *jedis* k ^"[Ljava.lang.String;" (into-array String vals)))

    :first-arg-is-connection
    (let [[conn k & vals] args]
      (.lpush conn k ^"[Ljava.lang.String;" (into-array String (rest vals))))))

(defn rpush
  ([^String k ^String v]
     (.rpush *jedis* k v))
  ([conn ^String k ^String v]
     (.rpush conn k v)))

(defn lset
  ([^String k ^Number i ^String v]
     (.lset *jedis* k (long i) v))
  ([conn ^String k ^Number i ^String v]
     (.lset conn k (long i) v)))

(defn llen
  ([^String k]
     (.llen *jedis* k))
  ([conn ^String k]
     (.llen conn k)))

(defn lindex
  ([^String k ^Number i]
     (.lindex *jedis* k (long i)))
  ([conn ^String k ^Number i]
     (.lindex conn k (long i))))

(defn lpop
  ([^String k]
     (.lpop *jedis* k))
  ([conn ^String k]
     (.lpop conn k)))

(defn blpop
  ([ks ^Number t]
     (if-let [pair (.blpop *jedis* (int t) ^"[Ljava.lang.String;" (into-array String ks))]
       (seq pair)))
  ([conn ks ^Number t]
     (if-let [pair (.blpop conn (int t) ^"[Ljava.lang.String;" (into-array String ks))]
       (seq pair))))

(defn rpop
  ([^String k]
     (.rpop *jedis* k))
  ([conn ^String k]
     (.rpop conn k)))

(defn brpop
  ([ks ^Number t]
     (if-let [pair (.brpop *jedis* (int t) ^"[Ljava.lang.String;" (into-array String ks))]
       (seq pair)))
  ([conn ks ^Number t]
     (if-let [pair (.brpop conn (int t) ^"[Ljava.lang.String;" (into-array String ks))]
       (seq pair))))

(defn lrange
  ([k ^Number start ^Number end]
     (seq (.lrange *jedis* k (long start) (long end))))
  ([conn k ^Number start ^Number end]
     (seq (.lrange conn k (long start) (long end)))))

(defn ltrim
  ([k ^Number start ^Number end]
     (.ltrim *jedis* k (long start) (long end)))
  ([conn k ^Number start ^Number end]
     (.ltrim conn k (long start) (long end))))

                                        ; Sets

(defn sadd
  ([^String k ^String m]
     (.sadd *jedis* k m))
  ([conn ^String k ^String m]
     (.sadd conn k m)))

(defn srem
  ([^String k ^String m]
     (.srem *jedis* k m))
  ([conn ^String k ^String m]
     (.srem conn k m)))

(defn spop
  ([^String k]
     (.spop *jedis* k))
  ([conn ^String k]
     (.spop conn k)))

(defn scard
  ([^String k]
     (.scard *jedis* k))
  ([conn ^String k]
     (.scard conn k)))

(defn smembers
  ([^String k]
     (seq (.smembers *jedis* k)))
  ([conn ^String k]
     (seq (.smembers conn k))))

(defn sismember
  ([^String k ^String m]
     (.sismember *jedis* k m))
  ([conn ^String k ^String m]
     (.sismember conn k m)))

(defn srandmember
  ([^String k]
     (.srandmember *jedis* k))
  ([conn ^String k]
     (.srandmember conn k)))

(defn smove
  ([^String k ^String d ^String m]
     (.smembers *jedis* k d m))
  ([conn ^String k ^String d ^String m]
     (.smembers conn k d m)))


                                        ; Sorted sets

(defn zadd
  ([^String k ^Double r ^String m]
     (.zadd *jedis* k r m))
  ([conn ^String k ^Double r ^String m]
     (.zadd conn k r m)))

(defn zcount
  ([^String k ^Double min ^Double max]
     (.zcount *jedis* k min max))
  ([conn ^String k ^Double min ^Double max]
     (.zcount conn k min max)))

(defn zcard
  ([^String k]
     (.zcard *jedis* k))
  ([conn ^String k]
     (.zcard conn k)))

(defn zrank
  ([^String k ^String m]
     (.zrank *jedis* k m))
  ([conn ^String k ^String m]
     (.zrank conn k m)))

(defn zrevrank
  ([^String k ^String m]
     (.zrevrank *jedis* k m))
  ([conn ^String k ^String m]
     (.zrevrank conn k m)))

(defn zscore
  ([^String k ^String m]
     (.zscore *jedis* k m))
  ([conn ^String k ^String m]
     (.zscore conn k m)))

;; NB: HERE
(defn zrangebyscore
  ([^String k ^Double min ^Double max]
     (seq (.zrangeByScore *jedis* k (double min) (double max))))
  ([^String k ^Double min ^Double max ^Number offset ^Number count]
     (seq (.zrangeByScore *jedis* k (double min) (double max) (int offset) (int count)))))

;; NB: HERE
(defn zrangebyscore-withscore
  ([^String k ^Double min ^Double max]
     (seq (.zrangeByScoreWithScore *jedis* k (double min) (double max))))
  ([^String k ^Double min ^Double max ^Number offset ^Number count]
     (seq (.zrangeByScoreWithScore *jedis* k (double min) (double max) (int offset) (int count)))))

(defn zrange
  ([^String k ^Number start ^Number end]
     (seq (.zrange *jedis* k (long start) (long end))))
  ([conn ^String k ^Number start ^Number end]
     (seq (.zrange conn k (long start) (long end)))))

(defn zrevrange
  ([^String k ^Number start ^Number end]
     (seq (.zrevrange *jedis* k (long start) (long end))))
  ([conn ^String k ^Number start ^Number end]
     (seq (.zrevrange conn k (long start) (long end)))))

(defn zincrby
  ([^String k ^Double s ^String m]
     (.zincrby *jedis* k (double s) m))
  ([conn ^String k ^Double s ^String m]
     (.zincrby conn k (double s) m)))

(defn zrem
  ([^String k ^String m]
     (.zrem *jedis* k m))
  ([conn ^String k ^String m]
     (.zrem conn k m)))

(defn zremrangebyrank
  ([^String k ^Number start ^Number end]
     (.zremrangeByRank *jedis* k (long start) (long end)))
  ([conn ^String k ^Number start ^Number end]
     (.zremrangeByRank conn k (long start) (long end))))

(defn zremrangebyscore
  ([^String k ^Double start ^Double end]
     (.zremrangeByScore *jedis* k (double start) (double end)))
  ([conn ^String k ^Double start ^Double end]
     (.zremrangeByScore conn k (double start) (double end))))

(defn zinterstore
  ([^String d k]
     (.zinterstore *jedis* d ^"[Ljava.lang.String;" (into-array String k)))
  ([conn ^String d k]
     (.zinterstore conn d ^"[Ljava.lang.String;" (into-array String k))))

(defn zunionstore
  ([^String d k]
     (.zunionstore *jedis* d ^"[Ljava.lang.String;" (into-array String k)))
  ([conn ^String d k]
     (.zunionstore conn d ^"[Ljava.lang.String;" (into-array String k))))


;; Hashes
(defn hget
  ([^String k ^String f]
     (.hget *jedis* k f))
  ([conn ^String k ^String f]
     (.hget conn k f)))

(defn hmget [& args]
  (cond
    (isa? (class (first args)) String)
    (let [[hash-name & keys] args]
      (seq (.hmget *jedis* hash-name ^"[Ljava.lang.String;" (into-array String keys))))

    :first-arg-is-connection
    (let [[conn hash-name & keys] args]
      (seq (.hmget conn hash-name ^"[Ljava.lang.String;" (into-array String keys))))))

(defn hset
  ([^String k ^String f ^String v]
     (.hset *jedis* k f v))
  ([conn ^String k ^String f ^String v]
     (.hset conn k f v)))

(defn hmset
  ([^String k h]
     (.hmset *jedis* k h))
  ([conn ^String k h]
     (.hmset conn k h)))

(defn hsetnx
  ([^String k ^String f ^String v]
     (.hsetnx *jedis* k f v))
  ([conn ^String k ^String f ^String v]
     (.hsetnx conn k f v)))

(defn hincrby
  ([^String k ^String f ^Number v]
     (.hincrBy *jedis* k f (long v)))
  ([conn ^String k ^String f ^Number v]
     (.hincrBy conn k f (long v))))

(defn hexists
  ([^String k ^String f]
     (.hexists *jedis* k f))
  ([conn ^String k ^String f]
     (.hexists conn k f)))

(defn hdel
  ([^String k ^String f]
     (.hdel *jedis* k (into-array String [f])))
  ([conn ^String k ^String f]
     (.hdel conn k (into-array String [f]))))

(defn hlen
  ([^String k]
     (.hlen *jedis* k))
  ([conn ^String k]
     (.hlen conn k)))

(defn hkeys
  ([^String k]
     (.hkeys *jedis* k))
  ([conn ^String k]
     (.hkeys conn k)))

(defn hvals
  ([^String k]
     (hvals *jedis* k))
  ([conn ^String k]
     (cond
       (isa? (class conn) Jedis)
       (seq (.hvals ^Jedis conn k))

       :pipelined
       (.hvals ^redis.clients.jedis.Pipeline conn k))))

(defn hgetall
  ([^String k]
     (hgetall *jedis* k))
  ([conn ^String k]
     (cond
       (isa? (class conn) Jedis)
       (.hgetAll ^Jedis conn k)

       :pipelined
       (.hgetAll ^redis.clients.jedis.Pipeline conn k))))


                                        ; Pub-Sub

(defn publish
  ([^String c ^String m]
     (publish *jedis* c m))
  ([conn ^String c ^String m]
     (cond
       (isa? (class conn) Jedis)
       (.publish ^Jedis conn c m)

       :pipelined
       (.publish ^redis.clients.jedis.Pipeline conn c m))))

(defn subscribe
  ([chs handler]
     (let [^JedisPubSub pub-sub (proxy [JedisPubSub] []
                                  (onSubscribe [ch cnt])
                                  (onUnsubscribe [ch cnt])
                                  (onMessage [ch msg] (handler ch msg)))]
       (.subscribe ^Jedis *jedis* pub-sub ^"[Ljava.lang.String;" (into-array String chs))))
  ([^Jedis conn chs handler]
     (let [^JedisPubSub pub-sub (proxy [JedisPubSub] []
                                  (onSubscribe [ch cnt])
                                  (onUnsubscribe [ch cnt])
                                  (onMessage [ch msg] (handler ch msg)))]
       (.subscribe conn pub-sub ^"[Ljava.lang.String;" (into-array String chs)))))

(defn- pipelined-results-seq [^java.util.List r]
  (map
   #(.get r %)
   (range (.size r))))

(defn transaction*
  "Creates a new transaction (See Jedis.pipelined) on the current connection and executes f in the transaction (wrapped with multi and exec).  Returns the sequence of results of executing the transaction."
  ([f]
     (transaction* *jedis* f))
  ([conn f]
     (let [^redis.clients.jedis.Pipeline p (.pipelined ^Jedis conn)]
       (.multi p)
       (binding [*jedis* p]
         (f))
       (let [exec-result (.exec p)
             results     (.syncAndReturnAll p)]
         (pipelined-results-seq results)))))

(defmacro transaction
  "Creates a new transaction (See Jedis.pipelined) on the current connection and executes body in the transaction (wrapped with multi and exec)."
  [& body]
  `(transaction*
    (fn []
      ~@body)))


(comment

  (require 'teporingo.redis)
  (teporingo.redis/register-redis-pool :local)
  (teporingo.redis/with-jedis :local
    *jedis*)

  )