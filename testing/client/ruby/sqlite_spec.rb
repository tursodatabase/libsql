require "sqlite3"

db_uri = ENV["DB_URI"]

if db_uri == ""
  throw "Please configure database via DB_URI environment variable."
end

describe "SQLite3 client" do
  it "connects" do
    db = SQLite3::Database.open db_uri
  end

  it "performs schema changes" do
    db = SQLite3::Database.open db_uri
    db.execute("CREATE TABLE IF NOT EXISTS users (username TEXT, pass TEXT)")
  end

  it "queries tables" do
    db = SQLite3::Database.open db_uri
    db.execute("CREATE TABLE IF NOT EXISTS users (username TEXT, pass TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS users (username TEXT, pass TEXT)")
    db.execute("SELECT * FROM users") do |results|
      puts results
    end
  end
end
