(ns questdb.client
  (:require [next.jdbc :as jdbc]
            [jepsen [client :as client]])
  (:import (java.sql Connection)))

(defn open-conn!
  "Opens a connection to the QuestDB node."
  [test node]
  (let [conn-params {:host     node
                     :dbtype   "postgresql"
                     :port     (:db-port test)
                     :dbname   (:db-name test)
                     :user     (:db-user test)
                     :password (:db-password test)
                     :sslmode  "disable"}
        ds          (jdbc/get-datasource conn-params)
        conn        (jdbc/get-connection ds)]
    conn))

(defn close-conn!
  "Closes a connection to the DB."
  [^java.sql.Connection conn]
  (.close conn))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (let [c (open-conn! test node)]
      (assoc this
             :node node
             :conn c)))

  (setup! [this test])

  (invoke! [_ test op])

  (teardown! [this test])

  (close! [this test]
    (close-conn! (:conn this))))
