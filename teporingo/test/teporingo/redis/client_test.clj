(ns teporingo.redis.client-test
  (:require
   [teporingo.redis :as redis]
   [teporingo.redis.client :as r])
  (:use
   [teporingo.redis.bindings :only [*jedis*]]
   clojure.test))

;; NB: you must have a running redis instance to execute these tests

(defn with-jedis-fixture [f]
  (redis/register-redis-pool :default-pool)
  (redis/with-jedis :default-pool
    (f))
  (redis/unregister-redis-pool :default-pool))

(use-fixtures :each with-jedis-fixture)


(deftest test-transaction
  (r/set "foo" "1")
  (let [result (r/transaction
                (r/set  "foo" "1234")
                (r/incr "foo")
                (r/get  "foo")
                (is (= "1"
                       (redis/with-jedis :default-pool
                         (r/get "foo")))))]
    (is (= "1235" (r/get "foo")))))

(deftest test-mset
  (r/mset "foo" "9"
          "bar" "10")
  (is (=
       (r/mget "foo" "bar")
       ["9" "10"])))

(deftest test-msetnx
  (r/del "foo" "bar")
  (is (= 1 (r/msetnx
            "foo" "9"
            "bar" "10")))
  (is (= 0
         (r/msetnx
          "foo" "9"
          "bar" "10")))
  (is (=
       (r/mget "foo" "bar")
       ["9" "10"])))

(deftest test-getset
  (r/del "foo")
  (r/set "foo" "123")
  (r/getset "foo" "456"))

(deftest test-append
  (r/del "foo")
  (r/set "foo" "123")
  (is (= 6        (r/append "foo" "456")))
  (is (= "123456" (r/get    "foo"))))


(deftest test-getrange
  (r/set "foo" "1234567")
  (is (= "345" (r/getrange "foo" 2 4))))

(deftest test-setex
  (r/setex "foo" 1 "stuff")
  (Thread/sleep 1001)
  (is (nil? (r/get "foo"))))

(deftest test-lpush
  (r/del "lfoo")
  (r/lpush "lfoo" "a" "b" "c" "d")
  (is (= 4 (r/llen "lfoo")))
  (is (= ["d" "c" "b" "a"]
         (r/lrange "lfoo" 0 3))))

(comment
  (redis/register-redis-pool :default-pool)

  (redis/with-jedis :default-pool
    (r/lrange "lfoo" 0 100))


  (run-tests)

  )


