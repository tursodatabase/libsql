# End-to-end testing of sqld

This crate runs end-to-end tests against sqld clusters.
It uses [octopod](https://github.com/MarinPostma/octopod) for orchestrating clusters, and [cargo-insta](https://crates.io/crates/cargo-insta) for snapshot testing.

## Adding/updating tests
In order to run the tests you need to have podman 4 installed:
### Macos
with homebrew
```bash
brew install podman
podman machine init --cpus 8 --memory 8196 --rootful
podman machine start
```

you also need `cargo-insta`:

```bash
cargo install cargo-insta
```

Once this is done, you can run:
```bash
./run-macos.sh
```

This will run all the tests, and prepare the snapshots for review, you can now run:

```bash
cargo insta review
```

### Linux
Installing steps depends on the distribution, just make sure you have podman 4 installed:
```bash
podman --version
```
You then need to set up the podman API socket:

```bash
export SQLD_TEST_PODMAN_ADDR="unix:///var/run/podman.sock"
podman system service -t 0 $SQLD_TEST_PODMAN_ADDR&
```

once this is done, you can run:
```bash
cargo insta test
cargo insta review
```
