(ns questdb.core
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen
             [cli :as cli]
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [questdb.client :as client]
            [slingshot.slingshot :refer [try+ throw+]]))

(def quest-dir "/opt/questdb")
(def data-dir "/opt/data")
(def conf-dir (str data-dir "/conf"))
(def log-config (str conf-dir "/log.conf"))
(def log-dir (str data-dir "/log"))
(def log-file (str log-dir "/questdb.log"))

(defn configure!
  []
  (info "Configuring QuestDB" log-config)
  (c/su
   (c/exec :mkdir :-p conf-dir)
   (c/exec :echo
              (-> "log.conf"
                  io/resource
                  slurp)
           :> log-config)))

(defn questdb!
  "Runs QuestDB main script"
  [& args]
  (c/su
   (c/cd quest-dir
         (apply c/exec "bin/questdb.sh" args))))

(defn start!
  "Starts QuestDB."
  []
  (configure!)
  (questdb! :start :-d data-dir))

(defn stop!
  "Stops QuestDB."
  []
  (try+
   (questdb! :stop)
   (catch [:exit 55] e
     ; already not running
     nil))
  (c/su (c/exec :rm :-rf data-dir)))

(defn status
  "Returns the status of the QuestDB."
  []
  (questdb! :status))

(defn db
  "Quest DB version."
  [version]
  (reify
   db/DB
   (setup! [_ test node]
           (info node "installing Quest DB" version)
           (c/su
            (let [url (str "https://github.com/questdb/questdb/releases/download/" version
                           "/questdb-" version "-rt-linux-amd64.tar.gz")]
              (cu/install-archive! url quest-dir)))
           (start!)
           (client/wait-for-connection test node)
           (info node "QuestDB status is " (status)))

   (teardown! [_ test node]
              (info node "tearing down Quest DB")
              (stop!))

   db/LogFiles
   (log-files [_ test node]
              [log-file])))

(defn questdb-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name            "questdb"
          :os              debian/os
          :db              (db "6.5.5")
          :client          (questdb.client.Client. nil)
          :pure-generators true
          :nodes           ["n1"]
          :db-port         8812
          :db-name         "qdb"
          :db-user         "admin"
          :db-password     "quest"}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn questdb-test})
            args))
