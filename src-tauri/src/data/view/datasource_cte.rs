use std::collections::{
    HashMap,
    HashSet
};
use crate::data::datasource::Datasource;
use crate::data::column_type;
use crate::util::error::Error;


#[derive(Clone)]
pub struct DatasourceCteColumn {
    /// The expression for the column value.
    value_expr: String,

    /// The ordinal for the column value.
    pub value_ord: String
}

/// A constructor for a CTE that pulls columns from a datasource.
pub struct DatasourceCteConstructor {
    /// The main datasource.
    datasource: Datasource,

    /// The columns queried in this CTE.
    columns: HashMap<i64, DatasourceCteColumn>,

    /// Datasources that are dependent on this one.
    /// The value for a datasource is true if the child datasource is always grouped, and false if it is ever not grouped.
    child_datasources: HashSet<Datasource>,

    /// True if the values from this CTE are always in a collection.
    /// False if the values from this CTE are ever not in a collection.
    pub is_always_collection: bool 
}

impl DatasourceCteConstructor {
    /// Constructs a new CTE from the datasource.
    pub fn new(datasource: Datasource, is_collection: bool) -> Self {
        Self {
            datasource,
            columns: HashMap::new(),
            child_datasources: HashSet::new(),
            is_always_collection: is_collection
        }
    }

    /// Returns true if the datasource is a root datasource.
    pub fn is_root_datasource(&self) -> bool {
        if let Datasource::Table { .. } = self.datasource {
            true 
        } else {
            false
        }
    }

    /// Registers a child datasource of this datasource.
    pub fn add_child_datasource(&mut self, datasource: &Datasource) {
        self.child_datasources.insert(datasource.clone());
    }

    /// Builds the SQL statement for this CTE.
    pub fn build(&self) -> Result<String, Error> {
        Ok(format!(
            "
            SELECT
                -- The row OID of this row in the datasource
                t.OID AS {}_OID
                -- The table OID and table row OID of this row in the datasource 
                {}
                -- Columns from this datasource 
                {}
                -- Parent datasource OID, if applicable
                {}
                -- Columns from child datasources
                {}
            FROM TABLE{} t
            -- Join to multiselect table, if applicable
            {}
            -- Joins to child datasources
            {}
            WHERE NOT t.TRASH
            ",
            self.datasource.get_alias(),

            // Table for this datasource
            {
                let child_inheritor_datasources: Vec<String> = self.child_datasources.iter()
                    .filter_map(|child_datasource| {
                        if let Datasource::InheritorTable { .. } = child_datasource {
                            Some(child_datasource.get_alias())
                        } else {
                            None
                        }
                    })
                    .collect();
                let datasource_alias: String = self.datasource.get_alias();
                format!(
                    ", {} AS {datasource_alias}_TABLE_OID, {} AS {datasource_alias}_TABLE_ROW_OID",
                    if child_inheritor_datasources.len() > 0 {
                        format!(
                            "COALESCE({})",
                            child_inheritor_datasources.iter()
                                .fold(format!("{}", self.datasource.get_table_oid()?), |acc, e| format!("{e}_TABLE_OID, {acc}"))
                        )
                    } else {
                        format!("{}", self.datasource.get_table_oid()?)
                    },
                    if child_inheritor_datasources.len() > 0 {
                        format!(
                            "COALESCE({})",
                            child_inheritor_datasources.iter()
                                .fold(String::from("t.OID"), |acc, e| format!("{e}_TABLE_ROW_OID, {acc}"))
                        )
                    } else {
                        String::from("t.OID")
                    }
                )
            },

            // Columns from this datasource
            self.columns.iter()
                .map(|(_, col)| format!("{} AS {}", col.value_expr, col.value_ord))
                .fold(String::from(""), |acc, e| format!("{acc}, {e}")),
            
            // Parent datasource OID, if applicable
            match &self.datasource {
                Datasource::Table { .. }
                | Datasource::MasterTable { .. } => String::from(""),
                Datasource::InheritorTable { parent_datasource, .. } => 
                    format!(
                        ", t.MASTER{}_OID AS PARENT_{}_OID", 
                        parent_datasource.get_table_oid()?,
                        parent_datasource.get_alias()
                    ),
                Datasource::Column { parent_datasource, column } => {
                    match column.column_type {
                        column_type::ColumnType::Object { .. }
                        | column_type::ColumnType::Select { .. } => {
                            if self.datasource.get_table_oid()? == column.schema.oid {
                                // Inverted direction
                                format!(
                                    ", t.COLUMN{} AS PARENT_{}_OID",
                                    column.oid,
                                    parent_datasource.get_alias()
                                )
                            } else {
                                // Normal direction
                                String::from("")
                            }
                        }
                        column_type::ColumnType::Multiselect { .. } => {
                            format!(
                                ", m.TABLE{}_OID AS PARENT_{}_OID", 
                                parent_datasource.get_table_oid()?, 
                                parent_datasource.get_alias()
                            )
                        }
                        _ => {
                            return Err(Error::adhoc("Datasource cannot be derived from a non-Select, non-Object, non-Multiselect column!"));
                        }
                    }
                }
            },

            // Columns from child datasources
            self.child_datasources.iter()
                .map(|child_datasource| {
                    let child_datasource_alias = child_datasource.get_alias();
                    format!("{child_datasource_alias}.*")
                })
                .fold(String::from(""), |acc, e| format!("{acc}, {e}")),

            self.datasource.get_table_oid()?,

            // Join to multiselect table, if applicable
            match &self.datasource {
                Datasource::Column { column, .. } => {
                    match column.column_type {
                        column_type::ColumnType::Multiselect { .. } => 
                            format!(
                                "INNER JOIN MULTISELECT{} m ON m.TABLE{}_OID = t.OID", 
                                column.oid, 
                                self.datasource.get_table_oid()?
                            ),
                        _ => String::from("")
                    }
                }
                _ => String::from("")
            },

            // Joins to child datasources
            {
                let mut child_datasource_joins: String = String::from("");
                for child_datasource in self.child_datasources.iter() {
                    let child_datasource_alias: String = child_datasource.get_alias();
                    match child_datasource {
                        Datasource::MasterTable { table_oid, .. } => {
                            child_datasource_joins = format!(
                                "{child_datasource_joins} INNER JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID = t.MASTER{table_oid}_OID"
                            );
                        }
                        Datasource::InheritorTable { .. } => {
                            child_datasource_joins = format!(
                                "{child_datasource_joins} LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.PARENT_{}_OID = t.OID",
                                self.datasource.get_alias()
                            );
                        }
                        Datasource::Column { column, .. } => {
                            child_datasource_joins = format!(
                                "{child_datasource_joins} LEFT JOIN {} ON {}",
                                child_datasource.get_alias(),
                                match column.column_type {
                                    column_type::ColumnType::Multiselect { .. } => format!(
                                        "{child_datasource_alias}.PARENT_{}_OID = t.OID",
                                        self.datasource.get_alias()
                                    ),
                                    column_type::ColumnType::Object { .. }
                                    | column_type::ColumnType::Select { .. } => {
                                        if column.schema.oid == self.datasource.get_table_oid()? {
                                            // Normal direction
                                            format!(
                                                "{child_datasource_alias}.{child_datasource_alias}_OID = t.COLUMN{}",
                                                column.oid
                                            )
                                        } else {
                                            // Inverted direction
                                            format!(
                                                "{child_datasource_alias}.PARENT_{}_OID = t.OID",
                                                self.datasource.get_alias()
                                            )
                                        }
                                    }
                                    _ => {
                                        return Err(Error::adhoc("Datasource cannot be derived from a non-Select, non-Object, non-Multiselect column!"));
                                    }
                                }
                            );
                        }
                        _ => {
                            return Err(Error::adhoc("Child datasource cannot be a root table!"));
                        }
                    }
                }
                child_datasource_joins
            }
        ))
    }

    /*
    /// Gets all columns, from both this CTE and all child datasource CTEs.
    fn get_all_columns(&self, select_constructor: &SelectConstructor) -> Vec<DatasourceCteColumn> {
        let mut columns = Vec::from_iter(self.columns.values().map(|c| c.clone()));
        for child_datasource in self.child_datasources.iter() {
            columns.splice(columns.len()..columns.len(), select_constructor.cte_datasource[&child_datasource.get_alias()].get_all_columns(select_constructor));
        }
        return columns;
    }
    */

    /// Adds a primitive column to the CTE.
    /// Assumes that the column is owned by the schema of this datasource.
    pub fn add_primitive_column(&mut self, column_oid: i64) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                value_expr: format!("t.COLUMN{column_oid}"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}")
            });
        }
        return self.columns[&column_oid].clone();
    }

    /// Adds an object column to the CTE.
    pub fn add_object_column(&mut self, column_oid: i64) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                value_expr: format!("t.COLUMN{column_oid}"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}")
            });
        }
        return self.columns[&column_oid].clone();
    }

    /// Adds a select column to the CTE.
    pub fn add_select_column(&mut self, column_oid: i64) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                value_expr: format!("t.COLUMN{column_oid}"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}")
            });
        }
        return self.columns[&column_oid].clone();
    }

    /// Adds a multiselect column to the CTE.
    pub fn add_multiselect_column(&mut self, column_oid: i64) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                value_expr: format!("(GROUP_CONCAT(CAST({datasource_alias}_COLUMN{column_oid}_OID AS TEXT), ',') OVER (PARTITION BY t.OID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}")
            });
        }
        return self.columns[&column_oid].clone();
    }
}