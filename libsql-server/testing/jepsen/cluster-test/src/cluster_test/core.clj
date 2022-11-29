(ns cluster-test.core
  (:require [jepsen [cli :as cli]
             [tests :as tests]]))

(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
   :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         { :pure-generators  true }
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run!
    (merge (cli/single-test-cmd {:test-fn etcd-test})
           (cli/serve-cmd))
    args))
