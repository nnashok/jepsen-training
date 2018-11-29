(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [jepsen [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]
                    [generator :as gen]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+]]))

(def dir "/opt/etcd")
(def binary "etcd")
(def pidfile (str dir "/etcd.pid"))
(def logfile (str dir "/etcd.log"))


(defn node-url
  [node port]
  (str "http://" node ":" port))

(defn client-url
  [node]
  (node-url node 2379))

(defn peer-url
  [node]
  (node-url node 2380))

(defn initial-cluster
  [test]
  (->> test
       :nodes
       (map (fn [node]
              (str node "=" (peer-url node))))
       (str/join ",")))

(defn db
  "Constructs a database for the given etcd version"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info "Setting up etcd" version)
        (let [url (str "https://storage.googleapis.com/etcd/" version "/etcd-" version "-linux-amd64.tar.gz")]
          (cu/install-archive! url dir))
        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir dir}
          binary
          :--log-output                   :stderr
          :--name                         node
          :--listen-peer-urls             (peer-url node)
          :--listen-client-urls           (client-url node)
          :--advertise-client-urls        (client-url node)
          :--initial-cluster-state        :new
          :--initial-advertise-peer-urls  (peer-url node)
          :--initial-cluster              (initial-cluster test))
        (Thread/sleep 10000)))

    (teardown! [_ test node]
      (info "Tearing down etcd" version)
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn parse-long
  "Parses a string as a Long. Passes through nil"
  [s]
  (when s (Long/parseLong s)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (client-url node)
                                 {:timeout 5000})))

  (setup! [this test])

  (invoke! [_ test op]
    (case (:f op)
      :read (assoc op :type :ok, :value (parse-long (v/get conn "foo")))
      :write (do (v/reset! conn "foo" (:value op))
                 (assoc op :type, :ok))
      :cas (try+
             (let [[old new] (:value op)]
              (assoc op :type (if (v/cas! conn "foo" old new)
                                :ok
                                :fail)))
             (catch [:errorCode 100] ex
               (assoc op :type :fail, :error :not-found)))))

  (teardown! [this test])

  (close! [_ test]))


(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn etcd-test
  "Takes cli options and constructs a test map"
  [opts]
  (merge tests/noop-test
         opts
         {:name "etcd"
          :os debian/os
          :db (db "v3.1.5")
          :client (Client. nil)
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis nil)
                          (gen/time-limit 15))}))

; Dummy function to just pring args
;(defn -main
;  "Runs command line args!"
;  [ & args]
;  (prn "Hello world!", args))

; - indicates this is a special function, similar to the __ in other languages.
; This is the function being called by lein when we run 'lein run'
(defn -main
  "Runs command line args!"
  [ & args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn etcd-test})
            (cli/serve-cmd))
            args))
