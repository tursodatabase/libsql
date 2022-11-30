# Jepsen test suite

This directory contains the jepsen test suite for iku-turso.

The testing clojure code is under the `cluster-test` directory.

## Running the tests

The vagrant directory contains a vagrant file that sets up a machine with a cluster of lxc containers, and setup the necessary network to manage the cluster.

In the `vagrant` directory, run:

```bash
vagrant up
TERM=screen-256color vagrant ssh
```

This will drop you in a shell on the vagrant

once there, try to run `ssh root@n1`: this should drop you into one of the node's shell. Exit this node's shell.

Navigate to the `/jepsen` directory and run `lein run test` to run the jepsen test suite.
