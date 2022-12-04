(ns questdb.client
  (:require [clojure.tools.logging :refer :all]
            [next.jdbc :as jdbc]
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

(defn retry-connection
  [test node retrires]
  (if (> retrires 0)
    (try
      (info "Waiting for DB to become available")
      (let [conn (open-conn! test node)]
        (jdbc/execute-one! conn ["show tables"]))
      (catch org.postgresql.util.PSQLException e
        (Thread/sleep 1000)
        (retry-connection test node (dec retrires))))))

(defn wait-for-connection
  [test node]
  (retry-connection test node 10))

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
