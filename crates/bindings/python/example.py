import libsql

con = libsql.connect(":memory:")

cur = con.cursor()

res = cur.execute("SELECT 1")

print(res.fetchone())
