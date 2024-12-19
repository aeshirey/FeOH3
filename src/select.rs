use rusqlite::{OptionalExtension, ToSql};

use crate::OrmTable;

/// Runs the specified `query` with its parameters with the expectation that zero or one result may be returned.
pub fn select_one<T>(
    conn: &mut rusqlite::Connection,
    query: &str,
    params: &[&dyn ToSql],
) -> Result<Option<T>, rusqlite::Error>
where
    T: OrmTable,
{
    conn.query_row(query, params, |r| T::from_row(r)).optional()
}

/// Runs the specified `query` with its parameters, returning all relevant rows.
pub fn select_many<T>(
    conn: &mut rusqlite::Connection,
    query: &str,
    params: &[&dyn ToSql],
) -> Result<Vec<T>, rusqlite::Error>
where
    T: OrmTable,
{
    let mut stmt = conn.prepare(query)?;
    let mut rows = stmt.query(params)?;

    let mut results = Vec::new();
    while let Some(row) = rows.next()? {
        if let Ok(r) = T::from_row(row) {
            results.push(r);
        }
    }

    Ok(results)
}

/// Selects all rows from the given table.
pub fn select_all<T>(conn: &mut rusqlite::Connection) -> Result<Vec<T>, rusqlite::Error>
where
    T: OrmTable,
{
    let query = format!("SELECT * FROM {}", T::TABLE_NAME);
    select_many(conn, &query, &[])
}

/// Counts the number of rows from the table that match the specified `column_condition`.
///
/// # Example
/// ```
/// use sqlite_orm::*;
/// # fn example<Person: OrmTable>(conn: &mut rusqlite::Connection) {
/// let count = select_count::<Person>(conn, Some("age IS NULL OR age = 42"), &[]).unwrap();
/// # }
/// ```
pub fn select_count<T>(
    conn: &mut rusqlite::Connection,
    column_condition: Option<&str>,
    params: &[&dyn ToSql],
) -> Result<i64, rusqlite::Error>
where
    T: OrmTable,
{
    let query = match column_condition {
        Some(c) => format!("SELECT COUNT(*) FROM {} WHERE {c}", T::TABLE_NAME),
        None => format!("SELECT COUNT(*) FROM {}", T::TABLE_NAME),
    };

    conn.query_row(&query, params, |r| r.get(0))
}
