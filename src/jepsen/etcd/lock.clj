(ns jepsen.etcd.lock
  "Tests etcd locks by protecting a read-modify-write update to a set."
  (:require [clojure.set :as set]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.etcd [client :as c]]))

(def set-key
  "lock-set")

(def lock-name
  "lock-set-lock")

(def lease-ttl-seconds
  2)

(defn read-delay-ms
  []
  (+ 750 (rand-int 500)))

(defn current-set
  [conn test]
  (or (:value (c/get conn set-key {:serializable? (:serializable? test)}))
      #{}))

(defrecord LockSetClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/client test node)))

  (setup! [_ test]
    (c/put! conn set-key #{}))

  (invoke! [_ test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :read (assoc op
                     :type :ok
                     :value (current-set conn test))

        :add (let [lease     (c/grant-lease! conn lease-ttl-seconds)
                   lease-id  (:id lease)
                   keepalive (c/keep-lease-alive! conn lease-id)]
               (try
                 (let [lock (c/acquire-lock! conn lock-name lease-id)]
                   (try
                     (let [value' (-> (current-set conn test)
                                      (conj (:value op)))]
                       (Thread/sleep (read-delay-ms))
                       (c/put! conn set-key value')
                       (assoc op :type :ok))
                     (finally
                       (c/release-lock! conn (:key lock)))))
                 (finally
                   (try
                     (c/close! keepalive)
                     (finally
                       (c/revoke-lease! conn lease-id)))))))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn w
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))))

(defn r
  ([]
   {:type :invoke, :f :read, :value nil})
  ([final?]
   (cond-> (r)
     final? (assoc :final? true))))

(defn acknowledged-adds
  [history]
  (->> history
       (filter (fn [op]
                 (and (= :ok (:type op))
                      (= :add (:f op)))))
       (map :value)
       set))

(defn final-reads
  [history]
  (->> history
       (filter (fn [op]
                 (and (= :ok (:type op))
                      (= :read (:f op))
                      (:final? op))))))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [adds       (acknowledged-adds history)
            reads      (final-reads history)
            values     (map :value reads)
            agreed?    (apply = values)
            final-set  (when (and (seq values) agreed?) (first values))
            lost       (if final-set
                         (sort (set/difference adds final-set))
                         [])
            unexpected (if final-set
                         (sort (set/difference final-set adds))
                         [])
            valid?     (cond
                         (empty? reads) :unknown
                         (not agreed?)  false
                         (seq lost)     false
                         :else          true)]
        (cond-> {:valid?           valid?
                 :adds             (count adds)
                 :final-read-count (count reads)}
          final-set (assoc :final-set final-set)
          (not agreed?) (assoc :final-reads (vec values))
          (seq lost) (assoc :lost (vec lost))
          (seq unexpected) (assoc :unexpected (vec unexpected)))))))

(defn workload
  "A generator, client, and checker for an etcd lock-protected set test."
  [opts]
  {:client          (LockSetClient. nil)
   :checker         (checker)
   :generator       (gen/mix [(repeat (r)) (w)])
   :final-generator (gen/each-thread (r true))})
