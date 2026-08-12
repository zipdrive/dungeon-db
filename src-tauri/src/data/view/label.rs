use std::collections::HashMap;
use rusqlite::{
    Connection,
    Transaction
};
use crate::util::encode::{sql_encode_string, sql_json_encode_expr};
use crate::util::error::Error;
use crate::util::db::{sql_collect, sql_execute};
use crate::data::column;
use crate::data::column_type;
use crate::data::datasource::Datasource;
use crate::data::view::formula::{Formula, FormulaReturnType, FormulaExpression};
use crate::data::view::wrapper_cte::{WrapperCteColumns, WrapperCteConstructor, WrapperCteReportColumn, WrapperCteTableColumn};


struct NonRecursiveLabelExpression {
    /// The metadata for the column.
    column_metadata: column::FullMetadata,

    /// True if the column is required. False otherwise.
    is_required_expr: String,

    /// The expression for a plain label.
    plain_label_expr: String,

    /// The expression for a JSON label excluding columns from inheritor tables.
    json_nonpolymorphic_label_expr: String,

    /// The expression for a JSON label including columns from inheritor tables.
    json_polymorphic_label_expr: String,
}

impl NonRecursiveLabelExpression {
    /// Gets the JSON label for this column.
    fn get_json_label(&self) -> String {
        match &self.column_metadata.column_type {
            column_type::ColumnType::Object { .. } => self.json_polymorphic_label_expr.clone(),
            _ => self.json_nonpolymorphic_label_expr.clone()
        }
    }

    /// Constructs an expression for a plaintext label.
    fn construct_plain_label_expr(partition_expr: String, schema_columns: &Vec<Self>) -> String {
        let table_columns: Vec<&Self> = schema_columns.iter().filter(|table_column| table_column.is_required_expr == "TRUE").collect();
        if table_columns.len() == 0 {
            if partition_expr.contains(",") {
                String::from("'N/A'")
            } else {
                format!("IIF({partition_expr} IS NULL, NULL, 'N/A')")
            }
        } else if table_columns.len() == 1 {
            table_columns[0].plain_label_expr.clone()
        } else {
            String::from("NULL")
        }
    }
    
    /// Constructs an expression for a JSON object with no polymorphism.
    fn construct_nonpolymorphic_json_label_expr(partition_expr: String, schema_columns: &Vec<Self>) -> String {
        let table_columns: Vec<&Self> = schema_columns.iter().filter(|table_column| table_column.is_required_expr == "TRUE").collect();
        if table_columns.len() == 0 {
            if partition_expr.contains(",") {
                String::from("'N/A'")
            } else {
                format!("IIF({partition_expr} IS NULL, NULL, '\"N/A\"')")
            }
        } else if table_columns.len() == 1 {
            let json_label_expr: String = table_columns[0].get_json_label();
            format!("IIF({}, COALESCE({json_label_expr}, 'null'), {json_label_expr})", table_columns[0].is_required_expr)
        } else {
            format!(
                "
('{{ ' || CONCAT_WS(', ', {}) || ' }}')
                ",
                table_columns.iter()
                    .map(|table_column| {
                        format!(
                            "('\"{}\": ' || {})", 
                            sql_encode_string(&table_column.column_metadata.name),
                            {
                                let json_label_expr: String = table_column.get_json_label();
                                format!("IIF({}, COALESCE({json_label_expr}, 'null'), {json_label_expr})", table_column.is_required_expr)
                            }
                        )
                    })
                    .reduce(|acc, e| format!("{acc}, {e}"))
                    .unwrap()
            )
        }
    }

    /// Constructs an expression for a JSON object with polymorphism.
    fn construct_polymorphic_json_label_expr(partition_alias: String, table_columns: &Vec<Self>) -> String {
        if table_columns.len() == 0 {
            format!(
                "('\"' || (SELECT {} FROM METADATA_SCHEMA s WHERE s.OID = w.{partition_alias}_TABLE_SCHEMA_OID) || '\"')",
                sql_json_encode_expr(&String::from("s.NAME"))
            )
        } else if table_columns.len() == 1 {
            format!(
                "('{{ \"' || (SELECT {} FROM METADATA_SCHEMA s WHERE s.OID = w.{partition_alias}_TABLE_SCHEMA_OID) || '\": ' || {} || ' }}')",
                sql_json_encode_expr(&String::from("s.NAME")),
                {
                    let json_label_expr: String = table_columns[0].get_json_label();
                    format!("IIF({}, COALESCE({json_label_expr}, 'null'), {json_label_expr})", table_columns[0].is_required_expr)
                }
            )
        } else {
            format!(
                "
(
    '{{ \"' || (SELECT {} FROM METADATA_SCHEMA s WHERE s.OID = w.{partition_alias}_TABLE_SCHEMA_OID) 
        || '\": {{ ' || COALESCE(CONCAT_WS(', ', {}) || ' ', '') || '}} }}'
)
                ",
                sql_json_encode_expr(&String::from("s.NAME")),
                table_columns.iter()
                    .map(|table_column| {
                        format!(
                            "('\"{}\": ' || {})", 
                            sql_encode_string(&table_column.column_metadata.name),
                            {
                                let json_label_expr: String = table_column.get_json_label();
                                format!("IIF({}, COALESCE({json_label_expr}, 'null'), {json_label_expr})", table_column.is_required_expr)
                            }
                        )
                    })
                    .reduce(|acc, e| format!("{acc}, {e}"))
                    .unwrap()
            )
        }
    }

    /// Constructs non-recursive labels for a column on a table.
    fn construct_labels_for_table(conn: &Connection, table_column: WrapperCteTableColumn) -> Result<Self, Error> {
        Ok(match &table_column.column_metadata.column_type {
            column_type::ColumnType::Primitive(prim) => {
                // For primitives, construct a basic label expression for the value

                let scalar_type: FormulaReturnType = FormulaReturnType::from(prim.clone());
                let value_expr: String = format!("w.{}_COLUMN{}", table_column.datasource_alias, table_column.column_metadata.oid);
                let json_label_expr: String = scalar_type.construct_json_label_expr(&value_expr);
                Self {
                    column_metadata: table_column.column_metadata,
                    is_required_expr: table_column.is_required_expr,
                    plain_label_expr: scalar_type.construct_plain_label_expr(&value_expr),
                    json_nonpolymorphic_label_expr: json_label_expr.clone(),
                    json_polymorphic_label_expr: json_label_expr
                }
            }

            column_type::ColumnType::Object { .. }
            | column_type::ColumnType::Select { .. } => {
                // Use the keys in child_columns to construct a single JSON object
                let partition_alias: String = format!("{}_COLUMN{}", table_column.datasource_alias, table_column.column_metadata.oid);
                let partition_expr: String = format!("w.{partition_alias}_OID");

                let child_table_column_labels: Vec<Self> = match table_column.child_columns {
                    Some(columns) => Self::construct_labels(conn, columns)?,
                    _ => Vec::new()
                };

                Self {
                    column_metadata: table_column.column_metadata,
                    is_required_expr: table_column.is_required_expr,
                    plain_label_expr: Self::construct_plain_label_expr(
                        partition_expr.clone(), 
                        &child_table_column_labels),
                    json_nonpolymorphic_label_expr: Self::construct_nonpolymorphic_json_label_expr(
                        partition_expr, 
                        &child_table_column_labels
                    ),
                    json_polymorphic_label_expr: Self::construct_polymorphic_json_label_expr(
                        partition_alias,
                        &child_table_column_labels
                    )
                }
            }

            column_type::ColumnType::Multiselect { .. }
            | column_type::ColumnType::Subreport { .. } => {
                let key_labels: Vec<Self> = match table_column.child_columns {
                    Some(columns) => Self::construct_labels(conn, columns)?,
                    _ => Vec::new()
                };

                let item_partition_alias: String = format!("{}_COLUMN{}", table_column.datasource_alias, table_column.column_metadata.oid);
                let item_partition_expr: String = format!("w.{item_partition_alias}_OID");
                let item_json_label_expr: String = Self::construct_nonpolymorphic_json_label_expr(
                    item_partition_expr, 
                    &key_labels
                );
                let json_label_expr: String = format!(
                    "
(
    '[ ' 
        || (
            GROUP_CONCAT({item_json_label_expr}, ', ') OVER (PARTITION BY w.{}_OID)
        )
        || ' ]'
)
                    ",
                    table_column.datasource_alias
                );

                Self {
                    column_metadata: table_column.column_metadata,
                    is_required_expr: table_column.is_required_expr,
                    plain_label_expr: String::from("NULL"),
                    json_nonpolymorphic_label_expr: json_label_expr.clone(),
                    json_polymorphic_label_expr: json_label_expr
                }
            }

            column_type::ColumnType::Formula { formula, .. } => {
                let parsed_formula: Formula = Formula::parse(formula.clone())?;
                let expr: FormulaExpression = FormulaExpression::from(trans, &parsed_formula)?;

            }
        })
    }

    fn construct_labels_for_report(conn: &Connection, report_column: WrapperCteReportColumn, partition_expr: String) -> Result<Self, Error> {
        Ok(match &report_column.column_metadata.column_type {
            column_type::ColumnType::Subreport { .. } => {
                let key_labels: Vec<Self> = match report_column.child_columns {
                    Some(columns) => Self::construct_labels(conn, columns)?,
                    _ => Vec::new()
                };

                let item_json_label_expr: String = Self::construct_nonpolymorphic_json_label_expr(
                    partition_expr.clone(), 
                    &key_labels
                );
                let json_label_expr: String = format!(
                    "
(
    '[ ' 
        || (
            GROUP_CONCAT({item_json_label_expr}, ', ') OVER (PARTITION BY {partition_expr})
        )
        || ' ]'
)
                    "
                );

                Self {
                    column_metadata: report_column.column_metadata,
                    is_required_expr: String::from("TRUE"),
                    plain_label_expr: String::from("NULL"),
                    json_nonpolymorphic_label_expr: json_label_expr.clone(),
                    json_polymorphic_label_expr: json_label_expr
                }
            }

            column_type::ColumnType::Formula { formula, .. } => {
                
            }
            
            _ => {
                return Err(Error::OrphanedDataColumn { 
                    column_oid: report_column.column_metadata.oid, 
                    column_name: report_column.column_metadata.name 
                });
            }
        })
    }

    fn construct_labels(conn: &Connection, keys: WrapperCteColumns) -> Result<Vec<Self>, Error> {
        Ok(match keys {
            WrapperCteColumns::TableColumns { columns } => {
                let mut column_labels: Vec<Self> = Vec::new();
                for table_column in columns {
                    match table_column.recurses_back_to {
                        None => {
                            column_labels.push(
                                Self::construct_labels_for_table(conn, table_column)?
                            );
                        }
                        Some(_) => {
                            let is_null_expr: String = format!(
                                "{}_COLUMN{} IS NULL", 
                                table_column.datasource_alias, 
                                table_column.column_metadata.oid
                            );
                            let json_label_expr: String = format!("IIF({is_null_expr}, IIF({}, 'null', NULL), '\"...\"')", table_column.is_required_expr);
                            column_labels.push(Self {
                                plain_label_expr: format!("IIF({is_null_expr}, NULL, '...')"),
                                json_nonpolymorphic_label_expr: json_label_expr.clone(),
                                json_polymorphic_label_expr: json_label_expr,
                                column_metadata: table_column.column_metadata.clone(),
                                is_required_expr: table_column.is_required_expr.clone(),
                            });
                        }
                    }
                }
                column_labels
            }

            WrapperCteColumns::ReportColumns { columns, partition_expr } => {
                let mut column_labels: Vec<Self> = Vec::new();
                for report_column in columns {
                    column_labels.push(
                        Self::construct_labels_for_report(conn, report_column, partition_expr.clone())?
                    );
                }
                column_labels
            }
        })
    }
}


/// Constructs a SCHEMA{schema_oid}_LABEL_VIEW view to query a label for each row in the schema.
pub fn construct_label_view(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    // Add all parameters to a wrapper CTE
    let mut wrapper: WrapperCteConstructor = WrapperCteConstructor::new();
    let mut keys: WrapperCteColumns = wrapper.set_schema(trans, schema_oid, true)?;
    match &mut keys {
        WrapperCteColumns::TableColumns { columns } => {
            columns.sort_by_key(|table_column| table_column.column_metadata.ordering);
        }
        WrapperCteColumns::ReportColumns { columns, .. } => {
            columns.sort_by_key(|report_column| report_column.column_metadata.ordering);
        }
    }

    // Add all OIDs as selected columns
    let mut oids: HashMap<String, String> = HashMap::new();
    for (oid_alias, oid_expr) in wrapper.get_oids().into_iter() {
        oids.insert(oid_alias, oid_expr);
    }

    // Map from column alias to column expression
    let mut c: HashMap<String, String> = HashMap::from(oids.clone());
    if oids.len() > 0 {
        let partition_expr: String = oids.iter()
            .map(|(_, oid_expr)| oid_expr.clone())
            .reduce(|acc, e| format!("{acc}, {e}"))
            .unwrap();

        // Build the label expressions
        let json_keys: Vec<NonRecursiveLabelExpression> = NonRecursiveLabelExpression::construct_labels(trans, keys)?;

        // Add plain label
        c.insert(
            String::from("PLAIN_LABEL"),
            NonRecursiveLabelExpression::construct_plain_label_expr(
                partition_expr.clone(), 
                &json_keys
            )
        );

        // Add JSON label with no polymorphism
        c.insert(
            String::from("JSON_LABEL"),
            NonRecursiveLabelExpression::construct_nonpolymorphic_json_label_expr(
                partition_expr, 
                &json_keys
            )
        );

        // If is table, add JSON label with polymorphism + TABLE + TABLE_ROW
        match Datasource::check_default_datasource_transact(trans, schema_oid)? {
            Some(root_datasource) => {
                // Add JSON label with polymorphism
                c.insert(
                    String::from("OBJECT_LABEL"), 
                    NonRecursiveLabelExpression::construct_polymorphic_json_label_expr(
                        root_datasource.get_alias(), 
                        &json_keys
                    )
                );

                // Add OID
                c.insert(
                    String::from("OID"),
                    format!("{}_OID", root_datasource.get_alias())
                );

                // Add TABLE
                c.insert(
                    String::from("TABLE_SCHEMA_OID"), 
                    format!("{}_TABLE_SCHEMA_OID", root_datasource.get_alias())
                );

                // Add TABLE_ROW
                c.insert(
                    String::from("TABLE_ROW_OID"), 
                    format!("{}_TABLE_ROW_OID", root_datasource.get_alias())
                );
            }
            _ => {}
        }
    }

    // Build and execute the CREATE VIEW expression
    let sql: String = format!(
        "
        CREATE VIEW IF NOT EXISTS SCHEMA{schema_oid}_LABEL_VIEW AS
        
        WITH {}
        {}
        ",

        // CTEs
        wrapper.build()?,

        // Top-level SELECT statement
        format!(
            "
            SELECT
                {}
            FROM WRAPPER w
            ",
            // Index, OIDs, and columns of the schema
            c.iter()
                .map(|(column_alias, column_expression)| format!("{column_expression} AS {column_alias}"))
                .reduce(|acc, e| format!("{acc}, {e}"))
                .unwrap()
        )
    );
    println!("\n{sql}\n");
    sql_execute(trans, sql, [])?;

    Ok(())
}