require 'sqlite3'

db_uri = ENV["DB_URI"]

if db_uri.nil?
    raise "Please configure database via the `DB_URI` environment variable."
end

db = SQLite3::Database.open db_uri

puts "Connected to `#{db_uri}` database"
puts ""

db.execute "CREATE TABLE IF NOT EXISTS bank_account (owner TEXT, balance DECIMAL)"
db.execute "DELETE FROM bank_account"
db.execute "INSERT INTO bank_account (owner, balance) VALUES ('alice', 150)"
db.execute "INSERT INTO bank_account (owner, balance) VALUES ('bob', 0)"

puts "Initial account balances:"

balances = db.query "SELECT owner, balance FROM bank_account"
while (balance = balances.next) do
    puts balance.join "\s"
end

db.execute "BEGIN"
db.execute "UPDATE bank_account SET balance = balance - 100 WHERE owner = 'alice'"
db.execute "UPDATE bank_account SET balance = balance + 100 WHERE owner = 'bob'"
db.execute "COMMIT"

puts ""
puts "Account balances after transaction:"

balances = db.query "SELECT owner, balance FROM bank_account"
while (balance = balances.next) do
    puts balance.join "\s"
end
