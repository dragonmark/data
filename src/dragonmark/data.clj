(ns dragonmark.data
  (:require
    [clojure.java.jdbc :as jdbc]
    [clj-postgresql.core :as pg]
    [clj-postgresql.types :as pgtypes]
    [cognitect.transit :as t]
    [clojure.string :as s :refer [join]]
    [taoensso.carmine :as car :refer (wcar)]
    ;;  [taoensso.carmine.ring :as car-ring]
    [cheshire.core :as cjson]
    [cheshire.generate :refer [JSONable]]
    [clojure.tools.logging :as log]
    [dragonmark.util.props :as dp])
  (:import (java.util UUID Map List Base64)
           (org.postgresql.util PGobject)
           (java.sql Connection SQLException)
           [com.fasterxml.jackson.core JsonGenerator]
           (java.io ByteArrayOutputStream ByteArrayInputStream)
           (clojure.lang Keyword)))

(set! *warn-on-reflection* true)

(defn- strip-hash
  "removes a trailing hash"
  [^String x]
  (if (or
        (.endsWith x "#")
        (.endsWith x "+")
        (.endsWith x "-"))
    (.substring x 0 (- (.length x) 1))
    x))

(defmulti ^UUID ensure-uuid "Makes sure the thing is a UUID" class)

(defmethod ensure-uuid nil
  [_]
  nil)

(defmethod ensure-uuid UUID
  [x]
  x)

(defmethod ensure-uuid Map
  [m]
  (ensure-uuid (:id m)))

(defmethod ensure-uuid String
  [s]
  (try (UUID/fromString s) (catch Exception _ nil)))



(def ensure-pg-crypto "The PostgreSQL statement to ensure the crypto libraries"
  "CREATE EXTENSION  IF NOT EXISTS \"pgcrypto\"")

(defonce db-conn-str-function-atom
  ;"A function that returns the Database connection information. `reset!` this atom early
  ;to do something other than `(-> @dp/info :db)`"
  (atom (fn [] (-> @dp/info :db))))

(def db-tables
  "An atom that contains table-name/metadata information about the DB's tables.
  Make sure to run `update-db-tables` after any schema operations"
  (atom {}))



(def ^:dynamic **currentent-db-connection** "The current JDBC connection" nil)

(declare update-db-tables)

;; The database connection
(defn- private-db
  []
  (let [the-fn @db-conn-str-function-atom
        the-val (and the-fn (the-fn))
        ret
        (if the-val
          (apply pg/pool (-> the-val seq flatten))
          (pg/pool))]
    ;; (future (update-db-tables))
    ret)
  )

(defmacro with-db-metadata
  "Evaluates body in the context of an active connection with metadata bound
   to the specified name. See also metadata-result for dealing with the results
   of operations that retrieve information from the metadata.
   (with-db-metadata [md db-spec]
     ... md ...)

     Differs from jdbc/with-db-metadata in that it does not close the connection"
  [binding & body]
  `(if (not (identical? ~(second binding) **currentent-db-connection**))
     (jdbc/with-db-metadata ~binding ~@body)
     (let [^java.sql.Connection con# (jdbc/get-connection ~(second binding))]
       (let [~(first binding) (.getMetaData con#)
             ret# (do ~@body)]
         ret#))))

(defn update-db-tables
  "Does the database have a given name"
  []
  (let [tables
        (->> (with-db-metadata
               [md (private-db)]
               (jdbc/metadata-result (.getTables md nil "public" nil nil)
                                     :result-set-fn identity))
             (filter #(= "TABLE" (:table_type %))))
        tables (into {} (mapcat (fn [t]
                                  [[(:table_name t) t]
                                   [(-> t :table_name keyword) t]])
                                tables))]
    (reset! db-tables tables)))

(defn db-conn
  "Returns the database connection"
  []
  (or **currentent-db-connection** (private-db)))

(defmacro in-transaction
  [& body]

  `(jdbc/with-db-transaction
     [trans# (private-db)]
     (binding [**currentent-db-connection** trans#]
       ~@body
       )))

(defn build-transaction-middleware
  "Builds ring middleware that wraps the current request
  in a transaction if it's not already in a transaction"
  [handler]
  (fn [request]
    (if **currentent-db-connection**
      (handler request)
      (in-transaction (handler request)))))

(defn run-statement
  "Runs an SQL statement"
  ([statement] (run-statement (db-conn) statement))
  ([db statement]
   (println "Statement " (pr-str statement))
   (cond
     (and (vector? statement)
          (= 2 (count statement)))
     (let [[table value] statement]
       (jdbc/insert! db table value))

     (string? statement)
     (jdbc/execute! db [statement])

     false
     (jdbc/with-db-connection
       [db db]
       (let [^Connection conn (:connection db)]
         (with-open [st (.createStatement conn)]
           (let [cnt
                 (.executeUpdate st statement)]
             (if (= 0 cnt)
               nil
               (with-open [rs (.getGeneratedKeys st)]
                 (if (.next rs)
                   (.getObject rs 1)
                   nil))))))))))

(defn statement-builder
  "Wrap a series of SQL statements into a function that can be applied against a JDBC connection"
  ([] (fn [db]))
  ([str]
   (fn [db] (run-statement db str)))
  ([s1 s2 & ss]
   (fn [db]
     (run-statement db s1)
     (run-statement db s2)
     (doseq [s ss] (run-statement db s)))))

(defmacro wcar* [& body] `(car/wcar (:redis @dp/info) ~@body))

(defn rpub
  "Publish a value to a Redis queue"
  [key value]
  (wcar* (car/publish key value)))

(defn rsub
  "Subscribe to a patterned redis queue such that each
time a message arrives on the queue, it's transit encoded
and passed to the function (in a future). The function returns
  a function that will unsubscribe the channel listener."
  [pat fnc]
  (let [listener (car/with-new-pubsub-listener (-> @dp/info :redis :spec)
                                               {pat (fn [msg]
                                                      (fnc msg))}
                                               (car/psubscribe pat))]
    (fn []
      (car/with-open-listener listener
                              (car/unsubscribe))
      (car/close-listener listener))))

(defn
  db-swap!
  "Runs a serializable transaction update a row.

  [db-connection table primary_key_to_morph function & parameters]
  "
  [table pk the-fn & rest]
  (let [pk (ensure-uuid pk)]
    (loop
      [cnt 0]
      (let
        [ret
         (try
           (jdbc/with-db-transaction
             [t-conn (db-conn)]
             (if-let
               [record
                (some->
                  (jdbc/query t-conn [(str "SELECT * FROM " (name table) " WHERE id = ?") pk]) first)]
               (let [morphed (apply the-fn record rest)]
                 (jdbc/update! t-conn table morphed ["id = ?" pk])
                 morphed)))
           (catch SQLException e
             (let [state (.getSQLState e)]
               (cond
                 (and (< cnt 10)                            ;; try 10 times
                      (or (= "40001" state)
                          (= "40P01" state)))
                 ::retry

                 :else (throw e)))))]
        (if (= ret ::retry) (recur (inc cnt))
                            (do
                              (rpub (str (name table) "." (.toString pk))
                                    "Updated")
                              ret))))))


(defn- process-order-by
  [field]
  (when field
    (let [field (name field)
          good (strip-hash field)]
      (str " ORDER BY " good " "
           (cond
             (.endsWith field "+")
             "ASCENDING"

             (.endsWith field "-")
             "DESCENDING"

             :else "")))))

(defn query
  "Thin veneer on jdbc/query: `(query :my_table {:id 44})` "
  [table q & {:keys [order_by]}]
  (jdbc/query (db-conn) (into
                          [(str "SELECT * FROM "
                                (name table)
                                " WHERE "
                                (join " AND " (map #(str (name %) " = ?") (keys q)))
                                (process-order-by order_by)
                                )]
                          (vals q)
                          )))

(defn by-id [table id]
  (->
    (query table {:id (ensure-uuid id)})
    first))

(defn update!
  [table record]
  (jdbc/update! (db-conn) table (dissoc record :id) ["id = ?" (ensure-uuid (:id record))])
  (rpub (str (name table) "." (-> record :id ensure-uuid .toString))
        "Updated")
  )

(defn insert!
  "Inserts a record into a table"
  [table record]
  (jdbc/insert! (db-conn) table record))

;(defn redis-sessions
;  "Build a Ring session handler for Redis. Optional number of minutes for an inactivity timeout.
;  Defaults to 30 minute timeout"
;  ([] (redis-sessions 30))
;  ([exp] (car-ring/carmine-store (:redis @dp/info) :expiration-secs (* exp 60))))

(defn redis-conn-info
  "Return connection information for redis"
  []
  (:redis @dp/info))

(defn rget
  "Get a key from Redis"
  [key]
  (wcar* (car/get key)))

(defn rput
  "put a value in Redis"
  [key value]
  (wcar* (car/set key value)))

(defn rbind-atom
  "Write the contents of an Atom to a key in Redis"
  [key a]
  ;; write changes to Redis
  (rput key @a)
  (add-watch a :watcher
             (fn [key atom
                  old-state
                  new-state]
               (future
                 (wcar*
                   (car/set key new-state))))))

(defmulti json-key "Is the type suitable as a JSON Key" class)

(defmulti json-value "Is the type suitable as a JSON value" class)

(defn json-kv
  [[k v]] (and (json-key k) (json-value v)))

(defmethod json-key :default [_] false)

(defmethod json-key String [_] true)

(defmethod json-key Keyword [_] true)

(defmethod json-value nil [_] true)

(defmethod json-value String [_] true)

(defmethod json-value Number [_] true)

(defmethod json-value Boolean [_] true)

(defmethod json-value List [lst] (every? json-value lst))

(defmethod json-value Map [mp] (every? json-kv mp))

(defmethod json-value :default [_] false)

(extend-protocol JSONable Object
  (to-json [t jg] (.writeNull ^JsonGenerator jg)))
;
;(defprotocol JSONable
;  (to-json [t jg]))

(defn transit-encode-mp
  "Encodes a Clojure data structre as Transit data"
  [data]
  (let [out (ByteArrayOutputStream. 4096)
        writer (t/writer out :msgpack)]
    (t/write writer data)
    (.encodeToString (Base64/getEncoder) (.toByteArray out))))

(defn transit-encode
  "Encodes a Clojure data structre as Transit data"
  [data]
  (let [out (ByteArrayOutputStream. 4096)
        writer (t/writer out :json)]
    (t/write writer data)
    (String. (.toByteArray out) "UTF-8")))

(defn transit-decode
  "Takes a String formatted as transit data and decodes it"
  [^String str]
  (let [in (ByteArrayInputStream. (.getBytes str "UTF-8"))
        reader (t/reader in :json)]
    (t/read reader)))

(defn transit-decode-mp
  "Takes a String formatted as transit data and decodes it"
  [^String str]
  (let [in (ByteArrayInputStream. (.decode (Base64/getDecoder) str))
        reader (t/reader in :msgpack)]
    (t/read reader)))

(defn special-json-encode
  "Encodes the data as JSON, but if it's going to lose fidelity, transit encode"
  [data]
  (if (and (instance? Map data)
           (not (json-value data)))
    (let [m (if (map? data)
              data
              (into {} data))]
      (cjson/encode (assoc m :$$transit (transit-encode-mp data))))
    (cjson/encode data)
    ))

(defn special-json-decode
  "Decodes JSON and if there's special transit data, that contains the actual full fidelity stuff"
  [val]
  (let [ret (cjson/decode val keyword)]
    (if (:$$transit ret) (transit-decode-mp (:$$transit ret)) ret)))

(defn- to-pg-json [data json-type]
  "Encode the data for JSON storage. If the data cannot
  be stored in full fidelity (e.g., there's a data or UUID),
  then transit encode the thing and put the transit encoded thing
  in `$$transit` key so PG JSON queries will work *and* we
  get full fidelity data back."
  (let [ed (special-json-encode data)]
    (doto (PGobject.)
      (.setType (name json-type))
      (.setValue ed))))

(defmethod pgtypes/map->parameter :jsonb
  [m _]
  (to-pg-json m :jsonb))

(defmethod pgtypes/read-pgobject :jsonb
  [^PGobject x]
  (when-let [val (.getValue x)]
    (special-json-decode val)))


(defn has-table?
  "Does the database have a given table"
  [conn ^String name]
  (let [tables (->> (with-db-metadata
                      [md conn]
                      (jdbc/metadata-result (.getTables md nil "public" nil nil)
                                            :result-set-fn identity))
                    (filter #(= "TABLE" (:table_type %)))
                    (map :table_name))
        table-set (into #{} tables)
        found (table-set (.toLowerCase name))]
    (boolean found)))

(defn get-columns
  "Gets the columns for the table"
  [conn ^String table]
  (let [columns
        (->> (with-db-metadata
               [md conn]
               (jdbc/metadata-result
                 (.getColumns md nil "public" table nil)
                 :result-set-fn identity)))]
    (into {} (map (fn [x] [(:column_name x) x]) columns))))


(defn get-indexes
  "Gets the indexs for the table"
  [conn ^String table]
  (let [indexes
        (->> (with-db-metadata [md conn]
                                    (jdbc/metadata-result
                                      (.getIndexInfo md nil "public" table false false)
                                      :result-set-fn identity)))]
    (into {} (map (fn [x] [(:index_name x) x]) indexes))))

(def field-type-map
  {"json" "jsonb"
   "date" "timestamp"})

(def field-modifiers
  {"pk"     "PRIMARY KEY DEFAULT gen_random_uuid()"
   "!null"  "NOT NULL"
   "unique" "UNIQUE"
   "now"    "DEFAULT now()"})

(defn field-fk
  "If the field reference is a foreign key reference, return that, otherwise nil"
  [^String m]
  (if (.contains m "%")
    (let [[t f] (-> (.split m "%") seq)]
      (str "REFERENCES " t "(" f ")"))
    ))

(defn compute-field-modifer
  [m]
  (or (field-modifiers m)
      (field-fk m)))

(defn build-field
  "Builds the information for a field"
  [n tpe]
  (let [[ft & mods] (-> tpe name (.split "\\.") vec)
        field-type (or (field-type-map ft) ft)
        the-name (-> n name .toLowerCase strip-hash)]
    {:name       the-name
     :type       :field
     :field-type field-type
     :cmd        (join " " (into [the-name
                                  field-type]
                                 (->> mods
                                      (map compute-field-modifer)
                                      (remove nil?))))}))



(defn build-index
  "Builds a set of index statements"
  [table i field-names]
  (let [field-names (->> field-names
                         (map name)
                         (filter #(.endsWith ^String % "#"))
                         (map #(-> ^String % .toLowerCase strip-hash)))]
    (letfn [(build-single-index
              [i]
              (let [is (-> i name (.split "\\."))
                    idx-name (.toLowerCase
                               (str (name table) "_"
                                    (join "_" is)))]
                {:name idx-name
                 :type :index
                 :cmd  (str "CREATE INDEX " idx-name " ON "
                            (name table)
                            " ("
                            (join ", " is)
                            ")")}))]
      (into
        []
        (concat
          (map build-single-index field-names)
          (cond
            (nil? i) []

            (-> i sequential? not)
            [(build-single-index i)]

            :else
            (map build-single-index i)))))))

(defn build-table
  "Tables a hash that contains table info and converts it into "
  [{:keys [$table $index] :as table}]
  (let [fields (filter (fn [[k _]] (-> k name (.startsWith "$") not)) table)
        fields (map #(apply build-field %) fields)
        table-name (-> $table name .toLowerCase)]
    {:name    table-name
     :fields  fields
     :type    :table
     :cmd     (str "CREATE TABLE "
                   table-name
                   " ("
                   (join ", " (map :cmd fields))
                   ")")
     :indexes (build-index $table $index (keys table))}))

(defn ensure-fields
  "Makes sure all the fields are on the table"
  [conn table fields]
  (let [columns (get-columns conn table)]
    (mapv (fn [{:keys [name cmd]}]
            (if (not (columns name))
              (str "ALTER TABLE " table " ADD COLUMN " cmd))) fields)))

(defn ensure-indexes
  "Make sure we have all the indexes"
  [conn table idxs]
  (let [cur-idxs (get-indexes conn table)]
    (mapv (fn [{:keys [name cmd]}]
            (if (not (cur-idxs name))
              cmd)) idxs)))

(defn- resolve-new-table
  "Resolve the information for $newtable"
  [{:keys [$newtable]}]
  (letfn [(process
            [it]
            (mapcat identity it))]
    (cond
      (sequential? $newtable)
      (process $newtable)

      (fn? $newtable)
      (process ($newtable))

      :else nil
      )
    ))


(defn schemify
  "Make sure the database is up to date with the table"
  [conn table-info]
  (let [ret (if (sequential? table-info)
          (mapcat (partial schemify conn) table-info)
          (let [info (build-table table-info)]
            (if (not (has-table? conn (:name info)))
              (into [ensure-pg-crypto (:cmd info)] (concat
                                                     (map :cmd (:indexes info))
                                                     (resolve-new-table table-info)
                                                     ))
              (remove nil? (concat
                             [ensure-pg-crypto]
                             (ensure-fields conn (:name info) (:fields info))
                             (ensure-indexes conn (:name info) (:indexes info))
                             )))
            ))]
    (if
      (= 1 (count ret))
      []
      (->>
        ret
        (map-indexed (fn [x y] (if (= y ensure-pg-crypto)
                                 (when (= 0 x) y)
                                 y
                                 )))
        (remove nil?)
        vec))))

(defn perform-schemification
  "Does everything for schemification including running the SQL"
  [table-info]
  (log/info (str "Performing schemification for " (pr-str table-info)))
  (in-transaction
    (let [cc **currentent-db-connection**
          ^Connection z (jdbc/db-find-connection cc)]
      (let [all (schemify **currentent-db-connection** table-info)]
        (log/info (str "Applying sql: " (pr-str all)))
        ((apply statement-builder all) **currentent-db-connection**))))
  (update-db-tables))