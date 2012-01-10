(ns teporingo.redis.client
  (:use
   teporingo.redis.bindings
   [clj-etl-utils.lang-utils :only [raise]])
  (:import
   [redis.clients.jedis
    Jedis JedisPool JedisPoolConfig JedisPubSub]))


;; teporingo.redis
;; this file: teporingo.redis.client (uses bindings)
;; also make  teporingo.redis.bindings

;; Copied from https://github.com/mmcgrana/clj-redis
(defn ping []
  (.ping ^Jedis *jedis*))

(defn flush-all []
  (.flushAll ^Jedis *jedis*))

;; Keys

(defn exists [^String k]
  (.exists ^Jedis *jedis* k))

(defn del [ks]
  (.del ^Jedis *jedis* ^"[Ljava.lang.String;" (into-array ks)))

(defn keys [& [^String pattern]]
  (seq (.keys ^Jedis *jedis* (or pattern "*"))))

(defn rename [^String k ^String nk]
  (.rename ^Jedis *jedis* k nk))

(defn renamenx [^String k ^String nk]
  (.renamenx ^Jedis *jedis* k nk))

(defn expire [^String k ^Integer s]
  (.expire ^Jedis *jedis* k s))

(defn expireat [^String k ^Long ut]
  (.expireAt ^Jedis *jedis* k ut))

(defn ttl [^String k]
  (.ttl ^Jedis *jedis* k))

(defn persist [^String k]
  (.persist ^Jedis *jedis* k))

(defn move [^String k ^Integer db]
  (.move ^Jedis *jedis* k db))

(defn type [^String k]
  (.type ^Jedis *jedis* k))


;; Strings

(defn incr [^String k]
  (.incr ^Jedis *jedis* k))

(defn incrby [^String k ^Long v]
  (.incrBy ^Jedis *jedis* k v))

(defn decr [^String k]
  (.decr ^Jedis *jedis* k))

(defn decrby [^String k ^Long v]
  (.decrBy ^Jedis *jedis* k v))

(defn get [^String k]
  (.get ^Jedis *jedis* k))

(defn set [^String k ^String v]
  (.set ^Jedis *jedis* k v))

(defn mget [& keys]
  (.mget ^Jedis *jedis* ^"[Ljava.lang.String;" (into-array keys)))

(defn mset [& keys]
  (.mset ^Jedis *jedis* ^"[Ljava.lang.String;" (into-array keys)))

(defn msetnx [& keys]
  (.msetnx ^Jedis *jedis* ^"[Ljava.lang.String;" (into-array keys)))

(defn getset [^String k ^String v]
  (.getSet ^Jedis *jedis* k v))

(defn append [^String k ^String v]
  (.append ^Jedis *jedis* k v))

(defn getrange [^String k ^Integer start ^Integer end]
  (.substring ^Jedis *jedis* k start end))

(defn setnx [^String k ^String v]
  (.setnx ^Jedis *jedis* k v))

(defn setex [^String k ^Integer s ^String v]
  (.setex ^Jedis *jedis* k s v))


                                        ; Lists

(defn lpush [^String k ^String v]
  (.lpush ^Jedis *jedis* k v))

(defn rpush [^String k ^String v]
  (.rpush ^Jedis *jedis* k v))

(defn lset [^String k ^Integer i ^String v]
  (.lset ^Jedis *jedis* k i v))

(defn llen [^String k]
  (.llen ^Jedis *jedis* k))

(defn lindex [^String k ^Integer i]
  (.lindex ^Jedis *jedis* k i))

(defn lpop [^String k]
  (.lpop ^Jedis *jedis* k))

(defn blpop [ks ^Integer t]
  (if-let [pair (.blpop ^Jedis *jedis* t ^"[Ljava.lang.String;" (into-array ks))]
    (seq pair)))

(defn rpop [^String k]
  (.rpop ^Jedis *jedis* k))

(defn brpop [ks ^Integer t]
  (if-let [pair (.brpop ^Jedis *jedis* t ^"[Ljava.lang.String;" (into-array ks))]
    (seq pair)))

(defn lrange
  [k ^Integer start ^Integer end]
  (seq (.lrange ^Jedis *jedis* k start end)))

(defn ltrim
  [k ^Integer start ^Integer end]
  (.ltrim ^Jedis *jedis* k start end))

                                        ; Sets

(defn sadd [^String k ^String m]
  (.sadd ^Jedis *jedis* k m))

(defn srem [^String k ^String m]
  (.srem ^Jedis *jedis* k m))

(defn spop [^String k]
  (.spop ^Jedis *jedis* k))

(defn scard [^String k]
  (.scard ^Jedis *jedis* k))

(defn smembers [^String k]
  (seq (.smembers ^Jedis *jedis* k)))

(defn sismember [^String k ^String m]
  (.sismember ^Jedis *jedis* k m))

(defn srandmember [^String k]
  (.srandmember ^Jedis *jedis* k))

(defn smove [^String k ^String d ^String m]
  (.smembers ^Jedis *jedis* k d m))


                                        ; Sorted sets

(defn zadd [^String k ^Double r ^String m]
  (.zadd ^Jedis *jedis* k r m))

(defn zcount [^String k ^Double min ^Double max]
  (.zcount ^Jedis *jedis* k min max))

(defn zcard [^String k]
  (.zcard ^Jedis *jedis* k))

(defn zrank [^String k ^String m]
  (.zrank ^Jedis *jedis* k m))

(defn zrevrank [^String k ^String m]
  (.zrevrank ^Jedis *jedis* k m))

(defn zscore [^String k ^String m]
  (.zscore ^Jedis *jedis* k m))

(defn zrangebyscore
  ([^String k ^Double min ^Double max]
     (seq (.zrangeByScore ^Jedis *jedis* k min max)))
  ([^String k ^Double min ^Double max ^Integer offset ^Integer count]
     (seq (.zrangeByScore ^Jedis *jedis* k min max offset count))))

(defn zrangebyscore-withscore
  ([^String k ^Double min ^Double max]
     (seq (.zrangeByScoreWithScore ^Jedis *jedis* k min max)))
  ([^String k ^Double min ^Double max ^Integer offset ^Integer count]
     (seq (.zrangeByScoreWithScore ^Jedis *jedis* k min max offset count))))

(defn zrange [^String k ^Integer start ^Integer end]
  (seq (.zrange ^Jedis *jedis* k start end)))

(defn zrevrange [^String k ^Integer start ^Integer end]
  (seq (.zrevrange ^Jedis *jedis* k start end)))

(defn zincrby [^String k ^Double s ^String m]
  (.zincrby ^Jedis *jedis* k s m))

(defn zrem [^String k ^String m]
  (.zrem ^Jedis *jedis* k m))

(defn zremrangebyrank [^String k ^Integer start ^Integer end]
  (.zremrangeByRank ^Jedis *jedis* k start end))

(defn zremrangebyscore [^String k ^Double start ^Double end]
  (.zremrangeByScore ^Jedis *jedis* k start end))

(defn zinterstore [^String d k]
  (.zinterstore ^Jedis *jedis* d ^"[Ljava.lang.String;" (into-array k)))

(defn zunionstore [^String d k]
  (.zunionstore ^Jedis *jedis* d ^"[Ljava.lang.String;" (into-array k)))


                                        ; Hashes

(defn hget [^String k ^String f]
  (.hget ^Jedis *jedis* k f))

(defn hmget [^String k & fs]
  (seq (.hmget ^Jedis *jedis* k ^"[Ljava.lang.String;" (into-array fs))))

(defn hset [^String k ^String f ^String v]
  (.hset ^Jedis *jedis* k f v))

(defn hmset [^String k h]
  (.hmset ^Jedis *jedis* k h))

(defn hsetnx [^String k ^String f ^String v]
  (.hsetnx ^Jedis *jedis* k f v))

(defn hincrby [^String k ^String f ^Long v]
  (.hincrBy ^Jedis *jedis* k f v))

(defn hexists [^String k ^String f]
  (.hexists ^Jedis *jedis* k f))

(defn hdel [^String k ^String f]
  (.hdel ^Jedis *jedis* k f))

(defn hlen [^String k]
  (.hlen ^Jedis *jedis* k))

(defn hkeys [^String k]
  (.hkeys ^Jedis *jedis* k))

(defn hvals [^String k]
  (seq (.hvals ^Jedis *jedis* k)))

(defn hgetall [^String k]
  (.hgetAll ^Jedis *jedis* k))


                                        ; Pub-Sub

(defn publish [^String c ^String m]
  (.publish ^Jedis *jedis* c m))

(defn subscribe [chs handler]
  (let [pub-sub (proxy [JedisPubSub] []
                  (onSubscribe [ch cnt])
                  (onUnsubscribe [ch cnt])
                  (onMessage [ch msg] (handler ch msg)))]
    (.subscribe ^Jedis *jedis* pub-sub ^"[Ljava.lang.String;" (into-array chs))))

(comment

  (teporingo.redis/register-redis-pool :local)

  (teporingo.redis/with-jedis :local
   (incr "foof")
   (expire "foof" 5))

  (teporingo.redis/with-jedis :local
   (get "foof"))

)