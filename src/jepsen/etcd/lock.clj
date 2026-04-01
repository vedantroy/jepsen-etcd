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

(defn add-op
  [x]
  {:type :invoke, :f :add, :value x})

(defn add-ops
  []
  (map add-op (range)))

(defn read-op
  []
  {:type :invoke, :f :read, :value nil})

(defn generator
  []
  (gen/mix [(add-ops)
            (repeat (read-op))]))

(defn sample-generated-ops
  [n]
  (loop [adds  (range)
         ops   []]
    (if (= n (count ops))
      ops
      (if (zero? (rand-int 2))
        (recur adds (conj ops (read-op)))
        (recur (rest adds) (conj ops (add-op (first adds))))))))

(defn cleanup!
  [conn keepalive lock-key lease-id]
  (when lock-key
    (try
      (c/release-lock! conn lock-key)
      (catch Throwable t
        (warn t "Failed to release lock"))))
  (when keepalive
    (try
      (c/close! keepalive)
      (catch Throwable t
        (warn t "Failed to close lease keepalive"))))
  (when lease-id
    (try
      (c/revoke-lease! conn lease-id)
      (catch Throwable t
        (warn t "Failed to revoke lease")))))

(defrecord LockSetClient [conn state]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/client test node)))

  (setup! [_ _test]
    (reset! state #{}))

  (invoke! [_ _test op]
    (case (:f op)
      :read (assoc op :type :ok, :value @state)

      :add (let [lease-id  (volatile! nil)
                 keepalive (volatile! nil)
                 lock-key  (volatile! nil)]
             (try+
               (let [{lease-id' :id} (c/grant-lease! conn 2)]
                 (vreset! lease-id lease-id')
                 (vreset! keepalive (c/keep-lease-alive! conn lease-id'))
                 (let [{lock-key' :key} (c/acquire-lock! conn lock-name lease-id')
                       current         @state]
                   (vreset! lock-key lock-key')
                   (Thread/sleep (rand-int (* 2 1000)))
                   (reset! state (conj current (:value op)))
                   (assoc op :type :ok)))
               (catch c/client-error? e
                 (assoc op
                        :type (if (:definite? e) :fail :info)
                        :error [(:type e) (:description e)]))
               (finally
                 (cleanup! conn @keepalive @lock-key @lease-id))))))

  (teardown! [_ _test])

  (close! [_ _test]
    (c/close! conn)))

(defn set-workload
  [_opts]
  {:client    (LockSetClient. nil (atom #{}))
   :checker   (checker/compose
                {:set      (checker/set-full {:linearizable? true})
                 :timeline (timeline/html)})
   :generator (generator)})
