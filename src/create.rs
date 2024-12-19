use crate::OrmTable;

/// Generates and executes a CREATE TABLE command.
pub fn create_table<T>(conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error>
where
    T: OrmTable,
{
    let mut q = format!("CREATE TABLE IF NOT EXISTS {} (", T::TABLE_NAME);

    for (name, r#type) in T::columns() {
        q.push_str(name);
        q.push(' ');
        q.push_str(r#type);
        q.push(',');
    }
    q.pop(); // last comma
    q.push(')');

    conn.execute(&q, [])?;

    // Create any supplemental indexes
    T::create_indexes(conn)?;

    Ok(())
}

/// Creates an index on the specified columns, in order. The name of the index is auto-generated.
///
/// # Panics
/// If `columns` is empty.
pub fn create_index<T>(
    conn: &mut rusqlite::Connection,
    columns: &[&'static str],
) -> Result<(), rusqlite::Error>
where
    T: OrmTable,
{
    assert!(!columns.is_empty());

    let mut query = format!("CREATE INDEX IF NOT EXISTS idx_{}", T::TABLE_NAME);
    for column in columns {
        query.push('_');
        query.push_str(column);
    }

    query.push_str(" ON ");
    query.push_str(T::TABLE_NAME);
    query.push_str(" (");
    for column in columns {
        query.push_str(column);
        query.push(',');
    }
    query.pop(); // last comma
    query.push(')');

    _ = conn.execute(&query, [])?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_create_index_works() {
        let mut conn = setup_db();

        assert!(create_index::<Person>(&mut conn, &["name"]).is_ok());
        assert!(create_index::<Person>(&mut conn, &["age"]).is_ok());
    }
}
