(defproject dragonmark/data "0.1.0"
  :description "Helpful utilities for PostgreSQL and Redis"
  :url "https://github.com/dragonmark/data"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [crypto-password "0.1.3"]
                 [org.clojure/java.jdbc "0.4.1"]
                 [com.cognitect/transit-clj "0.8.275"]
                 [com.cognitect/transit-cljs "0.8.220"]
                 [com.cognitect/transit-java "0.8.290"]
                 [com.taoensso/carmine "2.11.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-postgresql "0.4.0"]
                 [org.bitbucket.b_c/jose4j "0.4.4"]
                 [com.fasterxml.jackson.core/jackson-core "2.5.4"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.5.0"]
                 [commons-logging "1.1.3"]
                 [commons-codec "1.10"]

                 [dragonmark/util "0.1.3" :exclusions [org.clojure/clojure]]
                 ])
