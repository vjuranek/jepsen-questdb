(ns questdb.core
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def dir "/opt/questdb")

(defn db
  "Quest DB version."
  [version]
  (reify db/DB
         (setup! [_ test node]
                 (info node "installing Quest DB" version)
                 (c/su
                  (let [url (str "https://github.com/questdb/questdb/releases/download/" version
                                 "/questdb-" version "-rt-linux-amd64.tar.gz")]
                    (cu/install-archive! url dir))))

         (teardown! [_ test node]
                    (info node "tearing down Quest DB"))))

(defn questdb-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "questdb"
          :os   debian/os
          :db   (db "6.5.5")
          :pure-generators true
          :nodes ["n1"]}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn questdb-test})
            args))