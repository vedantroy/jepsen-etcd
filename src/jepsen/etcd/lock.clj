(ns jepsen.etcd.lock
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.etcd [client :as c]]
            [slingshot.slingshot :refer [try+]]))

(def lock-name
  "lock-set")

(defonce shared-set
  (atom #{}))

(defn r
  []
  {:type :invoke, :f :read, :value nil})

(defn adds
  []
  (->> (range)
       (map (fn [x]
              {:type :invoke, :f :add, :value x}))))

(defn cleanup-step!
  [stage f]
  (try+
    (f)
    (catch Object e
      (warn e "lock-set cleanup failed during" stage))))

(defrecord LockSetClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/client test node)))

  (setup! [_ test]
    (reset! shared-set #{}))

  (invoke! [_ test op]
    (case (:f op)
      :read
      (c/with-errors op #{:read}
        (assoc op :type :ok, :value @shared-set))

      :add
      (let [lease-id  (atom nil)
            keepalive (atom nil)
            lock-key  (atom nil)
            result    (try+
                        (c/remap-errors
                          (let [lease         (c/grant-lease! conn 2)
                                lease-id'     (:id lease)
                                _             (reset! lease-id lease-id')
                                keepalive'    (c/keep-lease-alive! conn lease-id')
                                _             (reset! keepalive keepalive')
                                lock          (c/acquire-lock! conn lock-name lease-id')
                                snapshot      @shared-set
                                _             (reset! lock-key (:key lock))
                                _             (Thread/sleep (rand-int (* 2 1000)))
                                updated-value (conj snapshot (:value op))]
                            (reset! shared-set updated-value)
                            ::wrote))
                        (catch c/client-error? e
                          (assoc op
                                 :type  (if (:definite? e) :fail :info)
                                 :error [(:type e) (:description e)]))
                        (finally
                          (when-let [lock-key' @lock-key]
                            (cleanup-step! :unlock
                                           #(c/release-lock! conn lock-key')))
                          (when-let [keepalive' @keepalive]
                            (cleanup-step! :keepalive-close
                                           #(c/close! keepalive')))
                          (when-let [lease-id' @lease-id]
                            (cleanup-step! :revoke
                                           #(c/revoke-lease! conn lease-id')))))]
        (if (= ::wrote result)
          (assoc op :type :ok)
          result))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn set-workload
  "A lock workload for lost acknowledged updates to Jepsen-local shared state."
  [opts]
  {:client    (LockSetClient. nil)
   :checker   (checker/compose
                {:set      (checker/set-full {:linearizable? true})
                 :timeline (timeline/html)})
   :generator (gen/mix [(adds) (repeat (r))])})
