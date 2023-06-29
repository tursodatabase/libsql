import libsql

con = libsql.connect("libsq://penberg.turso.io")

cur = con.cursor()

res = cur.execute("SELECT 1")

print(res.fetchone())
