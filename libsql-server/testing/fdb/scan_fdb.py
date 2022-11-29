import fdb
fdb.api_version(710)

db = fdb.open()
keys = dict(db[b'':b'\xff']).keys()
print(f"Keys in the database: {len(keys)}")
for key in keys:
    print(f"Entry {key} with len {len(db[key])}");
