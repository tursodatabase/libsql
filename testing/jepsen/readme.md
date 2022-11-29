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

This will drop you in a shell on the vagrant. You need to run as root, so run `sudo su` to get a root shell.

once there, try to run `ssh n1`: this should drop you in one of the node shell. exit this node shell.

navigate to the `/jepsen` directory and run `lein run test` to run the jepsen test suite.
