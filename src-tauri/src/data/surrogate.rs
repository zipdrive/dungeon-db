use std::collections::{HashMap, HashSet};
use rusqlite::{Transaction, params, types::Value, vtab::array::Array};
use crate::util::error::Error;

struct DependencyNode {
    table_oid: i64,

}

pub fn regenerate_surrogate(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    // Drop every surrogate view that is dependent on this table
    let mut dependencies: HashMap<i64, HashSet<i64>> = HashMap::new();
    drop_surrogate(trans, table_oid, &mut dependencies, Vec::new())?;

    // Re-create all of the surrogate views
    while dependencies.len() > 0 {
        let next_table_oid: i64 = {
            // Find the next table with no remaining dependencies
            let mut iter = dependencies.iter();
            loop { 
                if let Some((k_oid, k_dependencies)) = iter.next() {
                    // Create an arbitrary surrogate view that has no remaining dependent surrogate views that need to be created
                    if !k_dependencies.iter().any(|i| dependencies.contains_key(i)) {
                        break k_oid.clone();
                    }
                } else {
                    return Err(Error::AdhocError("The primary key is recursively self-referential!"));
                }
            }
        };
        dependencies.remove(&next_table_oid);
        create_surrogate(trans, next_table_oid)?;
    }
    Ok(())
}

/// Drops the surrogate views for all tables related to the given table or dependent on the surrogate view of the given table.
pub fn drop_surrogate(trans: &Transaction, table_oid: i64, dependencies: &mut HashMap<i64, HashSet<i64>>, dependent_on: Vec<HashSet<i64>>) -> Result<(), Error> {
    // Get all related table OIDs
    let related_table_oid_results: Vec<_> = trans.prepare(
            "
            SELECT ?1 AS OID
            UNION 
            SELECT MASTER_SCHEMA_OID AS OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE INHERITOR_SCHEMA_OID = ?1
            UNION
            SELECT INHERITOR_SCHEMA_OID AS OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1
            "
        )?
        .query_map(params![table_oid], |row| row.get::<_, i64>("OID"))?
        .collect();
    let mut related_table_oids: HashSet<i64> = HashSet::new();
    for related_table_oid_result in related_table_oid_results {
        related_table_oids.insert(related_table_oid_result?);
    }

    // Ensure there is not a recursive loop of primary keys referencing each other
    if dependent_on.iter().any(|dependent_on_table_oid| related_table_oids.intersection(dependent_on_table_oid).any(|_| true)) {
        if dependent_on.len() == 1 {
            return Err(Error::AdhocError("The primary key may be recursively self-referential!"));
        } else {
            return Err(Error::AdhocError("There is a recursive loop of primary keys on different tables that cause this primary key to be self-referential!"));
        }
    }

    // Drop every surrogate view that is dependent on this table or any related table
    if !dependencies.contains_key(&table_oid) {
        for related_table_oid in related_table_oids.iter() {
            dependencies.insert(related_table_oid.clone(), HashSet::new());
        }

        // Drop the surrogate views that are dependent on this table's surrogate view, because one of their primary keys is a reference to this table
        let dependent_table_oid_results: Vec<_> = trans.prepare(
                "
                SELECT
                    c.SCHEMA_OID
                FROM (
                    SELECT OID, TABLE_OID FROM MEATDATA_COLUMN_TYPE__OBJECT
                    UNION
                    SELECT OID, TABLE_OID FROM METADATA_COLUMN_TYPE__SELECT
                    UNION
                    SELECT OID, TABLE_OID FROM METADATA_COLUMN_TYPE__MULTISELECT
                ) ct ON ct.TABLE_OID IN ?1
                INNER JOIN METADATA_COLUMN c ON c.TYPE_OID = ct.OID
                WHERE c.IS_PRIMARY_KEY
                    AND NOT c.TRASH
                "
            )?
            .query_map(
                params![Array::new(related_table_oids.iter().map(|i| Value::Integer(i.clone())).collect())],
                |row| row.get::<_, i64>("SCHEMA_OID")
            )?
            .collect();
        let mut next_dependent_on: Vec<HashSet<i64>> = dependent_on.clone();
        next_dependent_on.push(related_table_oids.clone());
        for dependent_table_oid_result in dependent_table_oid_results {
            drop_surrogate(trans, dependent_table_oid_result?, dependencies, next_dependent_on.clone())?;
        }
    }
    
    // Add new dependencies to the dict
    if let Some(immediately_dependent_on_table_oids) = dependent_on.iter().last() {
        for related_table_oid in related_table_oids.iter() {
            if let Some(prior_dependencies) = dependencies.get_mut(related_table_oid) {
                prior_dependencies.extend(immediately_dependent_on_table_oids);
            }
        }
    }

    // Drop the surrogate views from every related table
    for related_table_oid in related_table_oids.iter() {
        let sql_drop: String = format!("DROP VIEW IF EXISTS TABLE{related_table_oid}_SURROGATE");
        trans.execute(&sql_drop, [])?;
    }
    Ok(())
}

/// Creates the surrogate view for the given table.
fn create_surrogate(trans: &Transaction, table_oid: i64) -> Result<(), Error> {

}