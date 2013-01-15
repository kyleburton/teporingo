(ns teporingo.core-test
  (:use
   teporingo.core
   clojure.test))

;; shoud not modify message if no TEP header is present
;; should remove TEP header if one is present
;; should remove multiple TEP headers if more than one is present

(deftest split-does-not-modify-non-tep-message
  (let [orig-msg "this is a message, w/no TEP message header"
        [tep-msg-id tep-msg-tstamp delivered-body] (split-body-and-msg-id (.getBytes orig-msg))]
    (is (nil? tep-msg-id))
    (is (nil? tep-msg-tstamp))
    (is (= orig-msg delivered-body))))

;; (split-does-not-modify-non-tep-message)

(deftest split-removes-single-tep-header
  (let [orig-msg                                   "this is the original message."
        amqp-payload                               (wrap-body-with-msg-id (.getBytes orig-msg))
        [tep-msg-id tep-msg-tstamp delivered-body] (split-body-and-msg-id amqp-payload)]
    (is (not (nil? tep-msg-id)))
    (is (not (nil? tep-msg-tstamp)))
    (is (= delivered-body orig-msg))))

;; (split-removes-single-tep-header)

;; NB: this is a temporary feature to handle a re-publish bug that
;; will be fixed in a future release
(deftest split-removes-multiple-tep-headers
  (let [orig-msg                                   "this is the original message."
        amqp-payload                               (wrap-body-with-msg-id (wrap-body-with-msg-id (.getBytes orig-msg)))
        [tep-msg-id tep-msg-tstamp delivered-body] (split-body-and-msg-id amqp-payload)]
    (is (not (nil? tep-msg-id)))
    (is (not (nil? tep-msg-tstamp)))
    (is (= delivered-body orig-msg))))

;; (split-removes-multiple-tep-headers)



