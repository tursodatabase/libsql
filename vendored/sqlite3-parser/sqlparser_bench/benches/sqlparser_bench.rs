// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use criterion::{criterion_group, criterion_main, Criterion};
use fallible_iterator::FallibleIterator;
use sqlite3_parser::lexer::sql::Parser;

fn basic_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlparser-rs parsing benchmark");

    let string = "SELECT * FROM `table` WHERE 1 = 1";
    group.bench_function("sqlparser::select", |b| {
        b.iter(|| {
            let mut parser = Parser::new(string.as_bytes());
            parser.next()
        });
    });

    let with_query = "
        WITH derived AS (
            SELECT MAX(a) AS max_a,
                   COUNT(b) AS b_num,
                   user_id
            FROM `TABLE`
            GROUP BY user_id
        )
        SELECT * FROM `table`
        LEFT JOIN derived USING (user_id)
    ";
    group.bench_function("sqlparser::with_select", |b| {
        b.iter(|| {
            let mut parser = Parser::new(with_query.as_bytes());
            parser.next()
        });
    });
}

criterion_group!(benches, basic_queries);
criterion_main!(benches);
