# LibSQL Python bindings

## Developing

Setup the development environment:

```
python3 -m venv .env
source .env/bin/activate
pip3 install maturin pyperf pytest
```

Build the development version and use it:

```
maturin develop && python3 example.py
```

Run the tests:

```
pytest
```
