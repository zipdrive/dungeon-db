use std::collections::HashMap;
use rusqlite::{
    Transaction,
    params
};
use crate::util::error::Error;
use crate::util::db::{sql_map_then_iter, sql_iter, sql_execute};
use crate::data::column_type;
use crate::data::column;
use crate::data::datasource::Datasource;
use crate::data::view::formula::FormulaReturnType;
use crate::data::view::wrapper_cte::{WrapperCteConstructor, WrapperCteColumns, WrapperCteTableColumn, WrapperCteReportColumn};

/// Constructs a SCHEMA{schema_oid}_VIEW to select all columns belonging to a schema.
pub fn construct_main_view(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    // Add all parameters to a wrapper CTE
    let mut wrapper: WrapperCteConstructor = WrapperCteConstructor::new();
    let columns: WrapperCteColumns = wrapper.set_schema(trans, schema_oid.clone(), false)?;

    // Map from column alias to column expression
    let mut c: HashMap<String, String> = HashMap::new();

    // Add all OIDs as selected columns
    for (oid_alias, oid_expr) in wrapper.get_oids().into_iter() {
        c.insert(oid_alias, oid_expr);
    }

    // Iterate over all columns of the schema
    match columns {
        WrapperCteColumns::TableColumns { columns: table_columns } => {
            for table_column in table_columns {
                match &table_column.column_metadata.column_type {
                    column_type::ColumnType::Primitive(prim) => {
                        let value_expr: String = format!(
                            "w.{}_COLUMN{}", 
                            table_column.datasource_alias,
                            table_column.column_metadata.oid
                        );

                        // Label expression
                        c.insert(
                            format!("COLUMN{}_LABEL", table_column.column_metadata.oid),
                            {
                                let scalar_type: FormulaReturnType = FormulaReturnType::from(prim.clone());
                                scalar_type.construct_plain_label_expr(&value_expr)
                            }
                        );
                        // Value expression
                        c.insert(
                            format!("COLUMN{}_VALUE", table_column.column_metadata.oid), 
                            value_expr
                        );
                    }

                    column_type::ColumnType::Object { table_oid, .. } => {
                        let value_expr: String = format!(
                            "w.{}_COLUMN{}", 
                            table_column.datasource_alias,
                            table_column.column_metadata.oid
                        );

                        // Label expression
                        c.insert(
                            format!("COLUMN{}_LABEL", table_column.column_metadata.oid),
                            format!("(SELECT l.OBJECT_LABEL FROM SCHEMA{table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})")
                        );
                        // Value expression
                        c.insert(
                            format!("COLUMN{}_VALUE", table_column.column_metadata.oid), 
                            value_expr
                        );
                    }

                    column_type::ColumnType::Select { table_oid, .. } => {
                        let value_expr: String = format!(
                            "w.{}_COLUMN{}", 
                            table_column.datasource_alias,
                            table_column.column_metadata.oid
                        );

                        // Label expression
                        c.insert(
                            format!("COLUMN{}_LABEL", table_column.column_metadata.oid),
                            format!("(SELECT COALESCE(l.PLAIN_LABEL, l.JSON_LABEL) FROM SCHEMA{table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})")
                        );
                        // Value expression
                        c.insert(
                            format!("COLUMN{}_VALUE", table_column.column_metadata.oid), 
                            value_expr
                        );
                    }

                    column_type::ColumnType::Multiselect { table_oid, .. } => {
                        // Label expression
                        c.insert(
                            format!("COLUMN{}_LABEL", table_column.column_metadata.oid),
                            format!(
                                "
                                '[' || (
                                    GROUP_CONCAT(
                                        (SELECT l.JSON_LABEL FROM SCHEMA{table_oid}_LABEL_VIEW l WHERE l.OID = w.{}_OID)
                                    ) OVER (
                                        PARTITION BY w.{}_OID BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                    )
                                ) || ']'
                                ",
                                table_column.datasource_alias,
                                table_column.datasource_alias
                            )
                        );
                        // Value expression
                        c.insert(
                            format!("COLUMN{}_VALUE", table_column.column_metadata.oid), 
                            format!(
                                "
                                GROUP_CONCAT(
                                    CAST(w.{}_COLUMN{}_OID AS TEXT)
                                ) OVER (
                                    PARTITION BY w.{}_OID BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                )
                                ", 
                                table_column.datasource_alias,
                                table_column.column_metadata.oid,
                                table_column.datasource_alias
                            )
                        );
                    }

                    column_type::ColumnType::Formula { formula, .. } => {
                        todo!("Compile SQL expression for formula in main view");
                    }

                    column_type::ColumnType::Subreport { report_oid, .. } => {
                        let mut value_expr_components: Vec<String> = Vec::new();
                        let mut label_expr_filters: Vec<String> = Vec::new();

                        // Collect the filters on the subreport
                        sql_iter(
                            trans,
                            format!("PRAGMA table_info(SCHEMA{report_oid}_LABEL_VIEW)"),
                            [],
                            |row| row.get::<_, String>("name"),
                            |subreport_column_name| {
                                if subreport_column_name.ends_with("_OID") {
                                    let subreport_datasource: Datasource = {
                                        let subreport_datasource_alias: String = subreport_column_name.replace("_OID", "");
                                        let table_datasource: Datasource = Datasource::from_alias_transact(trans, table_column.datasource_alias.clone())?;
                                        Datasource::from_alias_transact(trans, subreport_datasource_alias)?
                                            .substitute_root(table_datasource.get_table_oid()?, table_datasource)
                                    };

                                    // Check if the OID is present in this view
                                    if let Some(subreport_datasource_cte) = wrapper.get_datasource_cte(&subreport_datasource) {
                                        if !subreport_datasource_cte.is_always_collection {
                                            // The OID is present in the subreport and in this view, so add the OID as a filter on the subreport

                                            value_expr_components.push(format!(
                                                "SELECT '{subreport_column_name}=' || CAST(w.{}_OID AS TEXT)",
                                                subreport_datasource.get_alias()
                                            ));
                                            label_expr_filters.push(format!(
                                                "l.{subreport_column_name} = w.{}_OID",
                                                subreport_datasource.get_alias()
                                            ));
                                        }
                                    }
                                }
                                Ok(None::<()>)
                            }
                        )?;

                        // Construct the label expression
                        c.insert(
                            format!("COLUMN{}_LABEL", table_column.column_metadata.oid), 
                            if label_expr_filters.len() > 0 {
                                format!(
                                    "
                                    '[' || (
                                        GROUP_CONCAT(
                                            (SELECT l.JSON_LABEL FROM SCHEMA{report_oid}_LABEL_VIEW l WHERE {})
                                        )
                                    ) || ']'
                                    ",
                                    label_expr_filters.into_iter()
                                        .reduce(|acc, e| format!("{acc} AND {e}"))
                                        .unwrap()
                                )
                            } else {
                                String::from("NULL")
                            }
                        );

                        // Construct the value expression
                        c.insert(
                            format!("COLUMN{}_VALUE", table_column.column_metadata.oid),
                            if value_expr_components.len() > 0 {
                                format!(
                                    "GROUP_CONCAT(({}), '&')",
                                    value_expr_components.into_iter()
                                        .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                        .unwrap()
                                )
                            } else {
                                String::from("NULL")
                            }
                        );
                    }
                }
            }
        }
        WrapperCteColumns::ReportColumns { columns: report_columns, .. } => {
            for report_column in report_columns {
                match &report_column.column_metadata.column_type {
                    column_type::ColumnType::Formula { formula, .. } => {
                        todo!("Compile SQL expression for formula in main view");
                    }

                    column_type::ColumnType::Subreport { report_oid, .. } => {
                        let mut value_expr_components: Vec<String> = Vec::new();
                        let mut label_expr_filters: Vec<String> = Vec::new();

                        // Collect the filters on the subreport
                        sql_iter(
                            trans,
                            format!("PRAGMA table_info(SCHEMA{report_oid}_LABEL_VIEW)"),
                            [],
                            |row| row.get::<_, String>("name"),
                            |subreport_column_name| {
                                if subreport_column_name.ends_with("_OID") {
                                    let subreport_datasource: Datasource = {
                                        let subreport_datasource_alias: String = subreport_column_name.replace("_OID", "");
                                        Datasource::from_alias_transact(trans, subreport_datasource_alias)?
                                    };

                                    // Check if the OID is present in this view
                                    if let Some(subreport_datasource_cte) = wrapper.get_datasource_cte(&subreport_datasource) {
                                        if !subreport_datasource_cte.is_always_collection {
                                            // The OID is present in the subreport and in this view, so add the OID as a filter on the subreport

                                            value_expr_components.push(format!(
                                                "SELECT '{subreport_column_name}=' || CAST(w.{}_OID AS TEXT)",
                                                subreport_datasource.get_alias()
                                            ));
                                            label_expr_filters.push(format!(
                                                "l.{subreport_column_name} = w.{}_OID",
                                                subreport_datasource.get_alias()
                                            ));
                                        }
                                    }
                                }
                                Ok(None::<()>)
                            }
                        )?;

                        // Construct the label expression
                        c.insert(
                            format!("COLUMN{}_LABEL", report_column.column_metadata.oid), 
                            if label_expr_filters.len() > 0 {
                                format!(
                                    "
                                    '[' || (
                                        GROUP_CONCAT(
                                            (SELECT l.JSON_LABEL FROM SCHEMA{report_oid}_LABEL_VIEW l WHERE {})
                                        )
                                    ) || ']'
                                    ",
                                    label_expr_filters.into_iter()
                                        .reduce(|acc, e| format!("{acc} AND {e}"))
                                        .unwrap()
                                )
                            } else {
                                String::from("NULL")
                            }
                        );

                        // Construct the value expression
                        c.insert(
                            format!("COLUMN{}_VALUE", report_column.column_metadata.oid),
                            if value_expr_components.len() > 0 {
                                format!(
                                    "GROUP_CONCAT(({}), '&')",
                                    value_expr_components.into_iter()
                                        .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                        .unwrap()
                                )
                            } else {
                                String::from("NULL")
                            }
                        );
                    }
                    _ => {
                        return Err(Error::OrphanedDataColumn { 
                            column_oid: report_column.column_metadata.oid, 
                            column_name: report_column.column_metadata.name 
                        });
                    }
                }
            }
        }
    }

    // Add row index
    c.insert(
        String::from("ROW_INDEX"),
        format!(
            "ROW_NUMBER() OVER ({})",
            String::from("")
        )
    );

    // Build and execute the CREATE VIEW expression
    sql_execute(
        trans,
        format!(
            "
            CREATE VIEW IF NOT EXISTS SCHEMA{schema_oid}_VIEW AS
            
            WITH {}

            SELECT
                {}
            FROM WRAPPER w
            ",

            // CTEs
            wrapper.build()?,

            // Index, OIDs, and columns of the schema
            c.iter()
                .map(|(column_alias, column_expression)| format!("{column_expression} AS {column_alias}"))
                .reduce(|acc, e| format!("{acc}, {e}"))
                .unwrap()
        ),
        []
    )?;

    Ok(())
}