use crate::OrmTable;

pub fn insert_one<T>(conn: &mut rusqlite::Connection, record: &T) -> Result<(), rusqlite::Error>
where
    T: OrmTable,
{
    let mut q = format!("INSERT INTO {} (", T::TABLE_NAME);

    for (name, _) in T::columns() {
        q.push_str(name);
        q.push_str(", ");
    }
    q.pop(); // space
    q.pop(); // comma
    q.push_str(") VALUES (");

    // This seems entirely unnecessary:
    let params = record.values();
    //let params = params.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
    let params = &params[..];

    for i in 1..=params.len() {
        if i == 1 {
            q.push_str(&format!("?{i}"));
        } else {
            q.push_str(&format!(", ?{i}"));
        }
    }

    q.push(')');

    conn.execute(&q, params)?;
    Ok(())
}

pub fn insert_many<T>(conn: &mut rusqlite::Connection, records: &[T]) -> Result<(), rusqlite::Error>
where
    T: OrmTable,
{
    let q = {
        let mut q = format!("INSERT INTO {} (", T::TABLE_NAME);

        for (name, _column) in T::columns() {
            q.push_str(name);
            q.push_str(", ");
        }
        q.pop(); // space
        q.pop(); // comma
        q.push_str(") VALUES (");

        for i in 1..=T::columns().len() {
            if i == 1 {
                q.push_str(&format!("?{i}"));
            } else {
                q.push_str(&format!(", ?{i}"));
            }
        }

        q.push(')');
        q
    };

    let mut tx = conn.transaction()?;
    tx.set_drop_behavior(rusqlite::DropBehavior::Commit);

    let mut stmt = tx.prepare(&q)?;

    for record in records {
        let params = record.values();
        //let params = params.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
        let params = &params[..];
        stmt.execute(params)?;
    }

    Ok(())
}

/// Updates records in the table based on the single record specified.
///
/// In addition to passing the database connection and the record, you also must pass in
/// a (possibly empty) set of `identifiers` - column names that will be used to identify
/// which records must be updated - and a set non-empty set of `changed` columns - the
/// columns whose values will themselves be updated.
///
/// ```ignore
/// # use feoh3::*;
/// # fn example(conn: &mut rusqlite::Connection) {
/// let mut p = Person {
///     name: "Alice".to_string(),
///     age: None,
/// };
/// insert_one(conn, &p).unwrap();
/// p.age = Some(42);
/// update_one::<Person>(conn, p, &["name"], &["age"]).unwrap();
/// # }
/// ```
pub fn update_one<T>(
    conn: &mut rusqlite::Connection,
    record: &T,
    identifiers: &[&'static str],
    changed: &[&'static str],
) -> Result<(), rusqlite::Error>
where
    T: OrmTable,
{
    assert!(!changed.is_empty());

    let mut query = format!("UPDATE {} SET ", T::TABLE_NAME);

    for changed_name in changed {
        let changed_idx = T::columns()
            .iter()
            .enumerate()
            .find(|(_, (name, _))| name == changed_name)
            .expect("Couldn't find changed column name in table's columns")
            .0;

        query.push_str(&format!("{changed_name} = ?{}", changed_idx + 1));
    }

    if !identifiers.is_empty() {
        query.push_str(" WHERE ");

        for (i, id_name) in identifiers.iter().enumerate() {
            let id_idx = T::columns()
                .iter()
                .enumerate()
                .find(|(_, (name, _))| name == id_name)
                .expect("Couldn't find identifier column name in table's columns")
                .0;

            if i != 0 {
                query.push_str(" AND ");
            }

            query.push_str(&format!("{id_name} = ?{}", id_idx + 1));
        }
    }

    let params = record.values();
    _ = conn.execute(&query, &params[..])?;

    Ok(())
}

pub fn update_many<T>(
    conn: &mut rusqlite::Connection,
    records: &[T],
    identifiers: &[&'static str],
    changed: &[&'static str],
) -> Result<(), rusqlite::Error>
where
    T: OrmTable,
{
    assert!(!changed.is_empty());

    let query = {
        let mut query = format!("UPDATE {} SET ", T::TABLE_NAME);

        for changed_name in changed {
            let changed_idx = T::columns()
                .iter()
                .enumerate()
                .find(|(_, (name, _))| name == changed_name)
                .expect("Couldn't find changed column name in table's columns")
                .0;

            query.push_str(&format!("{changed_name} = ?{}", changed_idx + 1));
        }

        if !identifiers.is_empty() {
            query.push_str(" WHERE ");

            for (i, id_name) in identifiers.iter().enumerate() {
                let id_idx = T::columns()
                    .iter()
                    .enumerate()
                    .find(|(_, (name, _))| name == id_name)
                    .expect("Couldn't find identifier column name in table's columns")
                    .0;

                if i != 0 {
                    query.push_str(" AND ");
                }

                query.push_str(&format!("{id_name} = ?{}", id_idx + 1));
            }
        }

        query
    };

    let tx = conn.transaction()?;

    for record in records {
        let params = record.values();
        _ = tx.execute(&query, &params[..])?;
    }

    tx.commit()
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_insert() {
        let mut conn = setup_db();

        let people = vec![
            Person {
                name: "Alice".to_string(),
                age: None,
            },
            Person {
                name: "Bob".to_string(),
                age: Some(37),
            },
        ];

        insert_many(&mut conn, &people).unwrap();

        let num = select_count::<Person>(&mut conn, None, &[]).unwrap();
        assert_eq!(num, 2);
    }

    #[test]
    fn test_insert_update_selectall() {
        let mut conn = setup_db();

        let mut p = Person {
            name: "Alice".to_string(),
            age: None,
        };

        // Insert & verify
        {
            insert_one(&mut conn, &p).unwrap();
            let people = select_all::<Person>(&mut conn).unwrap();
            assert_eq!(people.len(), 1);

            assert_eq!(people[0].name, "Alice");
            assert_eq!(people[0].age, None);
        }

        // Update & verify
        {
            p.age = Some(42);
            update_one::<Person>(&mut conn, &p, &["name"], &["age"]).unwrap();

            let people = select_all::<Person>(&mut conn).unwrap();
            assert_eq!(people.len(), 1);

            assert_eq!(people[0].name, "Alice");
            assert_eq!(people[0].age, Some(42));
        }
    }

    #[test]
    fn test_update_many() {
        let mut people = vec![
            Person {
                name: "Alice".to_string(),
                age: Some(35),
            },
            Person {
                name: "Bob".to_string(),
                age: Some(37),
            },
        ];

        let mut conn = setup_db();
        insert_many(&mut conn, &people).unwrap();

        // Make some updates:
        people[0].age = Some(36); // 35->36 for alice
        people[1].age = Some(38); // 37->38 for bob
        update_many(&mut conn, &people, &["name"], &["age"]).unwrap();

        // Verify
        let people = select_many::<Person>(&mut conn, "SELECT * FROM Person ORDER BY age", &[]).unwrap();

        assert_eq!(people.len(), 2);

        assert_eq!(people[0].name, "Alice");
        assert_eq!(people[0].age, Some(36));

        assert_eq!(people[1].name, "Bob");
        assert_eq!(people[1].age, Some(38));
    }
}
