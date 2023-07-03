import libsql

con = libsql.connect(":memory:")

cur = con.cursor()

cur.execute("CREATE TABLE users (id INTEGER, email TEXT);")
cur.execute("INSERT INTO users VALUES (1, 'penberg@iki.fi')")

print(cur.execute("SELECT * FROM users").fetchone())
