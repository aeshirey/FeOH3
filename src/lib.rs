//! This library acts as a rudimentary [ORM](https://en.wikipedia.org/wiki/Object%E2%80%93relational_mapping), providing
//! the [OrmTable] trait that lets you simply model your struct in such a way that boilerplate SQL for SQLite can be
//! generated.

mod create;
pub use create::*;

mod insert;
pub use insert::*;

mod select;
pub use select::*;

use rusqlite::{self, Row, ToSql};

pub trait OrmTable
where
    Self: Sized,
{
    const TABLE_NAME: &'static str;
    fn columns() -> &'static [(&'static str, &'static str)];
    fn values(&self) -> Vec<&dyn ToSql>;

    fn from_row(row: &Row) -> Result<Self, rusqlite::Error>;

    #[allow(unused_variables)]
    fn create_indexes(conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
        Ok(())
    }
}

#[cfg(test)]
struct Person {
    name: String,
    age: Option<u8>,
}

#[cfg(test)]
impl OrmTable for Person {
    const TABLE_NAME: &'static str = "Person";

    fn columns() -> &'static [(&'static str, &'static str)] {
        &[("name", "TEXT NOT NULL"), ("age", "INTEGER NULL")]
    }

    fn values(&self) -> Vec<&dyn ToSql> {
        vec![&self.name, &self.age]
    }

    fn from_row(row: &Row) -> Result<Self, rusqlite::Error> {
        // Values can be indexed with the position (as it appears in OrmTable::columns!)...
        let name = row.get(0)?;

        // ...Or by name
        let age = row.get("age")?;

        Ok(Person { name, age })
    }
}

#[cfg(test)]
fn setup_db() -> rusqlite::Connection {
    let mut conn = rusqlite::Connection::open_in_memory().unwrap();

    create_table::<Person>(&mut conn).unwrap();
    create_index::<Person>(&mut conn, &["name"]).unwrap();

    conn
}
