var libsql = require('.');

var db = new libsql.Database(':memory:');

db.all('SELECT 1', function(err, res) {
  if (err) {
    throw err;
  }
  console.log(res[0])
});
