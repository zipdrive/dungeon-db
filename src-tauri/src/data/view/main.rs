use std::collections::HashMap;
use rusqlite::{
    Transaction,
    params
};
use crate::util::error::Error;
use crate::util::db::{sql_map_then_iter, sql_iter, sql_collect, sql_execute};
use crate::data::column_type;
use crate::data::column;
use crate::data::datasource::Datasource;
use crate::data::view::formula::{FormulaExpression, FormulaReturnType};
use crate::data::view::wrapper_cte::{WrapperCteConstructor, WrapperCteColumns, WrapperCteTableColumn, WrapperCteReportColumn};

fn replace_label_placeholders_in_formula(trans: &Transaction, expr: &mut FormulaExpression, child_columns: Option<WrapperCteColumns>) -> Result<(), Error> {
    // Replace label placeholders
    match child_columns {
        Some(WrapperCteColumns::TableColumns { columns: params }) => {
            for param in params {
                let label_placeholder: String = format!("__{}_COLUMN{}_LABEL__", param.datasource_alias, param.column_metadata.oid);
                let (plain_label_expr, json_label_expr) = match param.column_metadata.column_type {
                    column_type::ColumnType::Primitive(prim) => {
                        let scalar_type: FormulaReturnType = FormulaReturnType::from(prim);
                        let value_expr: String = format!("w.{}_COLUMN{}", param.datasource_alias, param.column_metadata.oid);
                        (
                            scalar_type.construct_plain_label_expr(&value_expr),
                            scalar_type.construct_json_label_expr(&value_expr)
                        )
                    }
                    column_type::ColumnType::Object { table_oid, .. } => {
                        let value_expr: String = format!("w.{}_COLUMN{}", param.datasource_alias, param.column_metadata.oid);
                        (
                            String::from("NULL"),
                            format!("(SELECT l.OBJECT_LABEL FROM SCHEMA{table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})")
                        )
                    }
                    column_type::ColumnType::Select { table_oid, .. } => {
                        let value_expr: String = format!("w.{}_COLUMN{}", param.datasource_alias, param.column_metadata.oid);
                        (
                            format!("(SELECT l.PLAIN_LABEL FROM SCHEMA{table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})"),
                            format!("(SELECT l.JSON_LABEL FROM SCHEMA{table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})")
                        )
                    }
                    column_type::ColumnType::Multiselect { table_oid, .. } => {
                        let partition_expr: String = format!("w.{}_OID", param.datasource_alias);
                        let value_expr: String = format!("w.{}_COLUMN{}_OID", param.datasource_alias, param.column_metadata.oid);
                        (
                            String::from("NULL"),
                            format!("('[ ' || (SELECT GROUP_CONCAT(l.JSON_LABEL, ', ') FROM SCHEMA{table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr} GROUP BY l.OID), ', ')) || ' ]')")
                        )
                    }
                    column_type::ColumnType::Subreport { report_oid, .. } => {
                        (
                            String::from("NULL"),
                            format!(
                                "('[ ' || (SELECT GROUP_CONCAT(l.JSON_LABEL, ', ') FROM SCHEMA{report_oid}_LABEL_VIEW l WHERE {} {}) || ' ]')",

                                // Filter expression
                                {
                                    return Err(Error::adhoc("Subreport parameters of formula are not yet implemented!"));
                                    String::from("FALSE")
                                },

                                // Partition expression
                                match Datasource::from_alias_transact(trans, param.datasource_alias)?
                                    .linearize()
                                    .into_iter()
                                    .map(|d| format!("w.{}_OID", d.get_alias()))
                                    .reduce(|acc, e| format!("{acc}, {e}")) {
                                    Some(partition_expr) => format!("GROUP BY {partition_expr}"),
                                    None => String::from("")
                                }
                            )
                        )
                    }
                    _ => {
                        return Err(Error::adhoc("Expected parameter of type Formula to be expanded!"));
                    }
                };
                expr.replace_label(label_placeholder, plain_label_expr, json_label_expr);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Constructs a SCHEMA{schema_oid}_VIEW to select all columns belonging to a schema.
pub fn construct_main_view(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    // Add all parameters to a wrapper CTE
    let mut wrapper: WrapperCteConstructor = WrapperCteConstructor::new();
    let columns: WrapperCteColumns = wrapper.set_schema(trans, schema_oid.clone(), false)?;

    // Map from column alias to column expression
    let mut c: HashMap<String, String> = HashMap::new();

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
    SELECT 
        GROUP_CONCAT(l.JSON_LABEL, ', ') 
    FROM SCHEMA{table_oid}_LABEL_VIEW l 
    WHERE l.OID = w.{}_OID
    GROUP BY l.{}_OID
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
    PARTITION BY w.{}_OID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
                                ", 
                                table_column.datasource_alias,
                                table_column.column_metadata.oid,
                                table_column.datasource_alias
                            )
                        );
                    }

                    column_type::ColumnType::Formula { formula, .. } => {
                        let mut expr: FormulaExpression = FormulaExpression::from(trans, formula)?;
                        replace_label_placeholders_in_formula(trans, &mut expr, table_column.child_columns)?;

                        // Value expression
                        c.insert(
                            format!("COLUMN{}_VALUE", table_column.column_metadata.oid),
                            expr.value_expr
                        );
                        // Label expression
                        c.insert(
                            format!("COLUMN{}_LABEL", table_column.column_metadata.oid),
                            format!("COALESCE({}, {})", expr.plain_label_expr, expr.json_label_expr)
                        );
                        // Cell expression
                        c.insert(
                            format!("COLUMN{}_CELL", table_column.column_metadata.oid), 
                            expr.cell_expr
                        );
                        // Isolated reload expression
                        c.insert(
                            format!("COLUMN{}_ISOLATEDRELOAD", table_column.column_metadata.oid),
                            expr.isolated_reload_cells.into_iter()
                                .map(|e| format!("COALESCE({e} || ',', '')"))
                                .reduce(|acc, e| format!("{acc} || {e}"))
                                .unwrap_or(String::from("''"))
                        );
                        // Full reload expression
                        c.insert(
                            format!("COLUMN{}_FULLRELOAD", table_column.column_metadata.oid),
                            expr.full_reload_cells.into_iter()
                                .map(|e| format!("COALESCE({e} || ',', '')"))
                                .reduce(|acc, e| format!("{acc} || {e}"))
                                .unwrap_or(String::from("''"))
                        );
                    }

                    column_type::ColumnType::Subreport { report_oid, .. } => {
                        let mut value_expr_components: Vec<String> = Vec::new();
                        let mut label_expr_filters: Vec<String> = Vec::new();

                        // Collect the filters on the subreport
                        sql_iter(
                            trans,
                            format!("PRAGMA table_info(SCHEMA{report_oid}_LABEL_VIEW)"),
                            [],
                            |row| {
                                let subreport_column_name: String = row.get::<_, String>("name")?;
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
                                                "SELECT '{subreport_column_name}=' || CAST(w.{}_OID AS TEXT) AS QF",
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
    SELECT 
        GROUP_CONCAT(l.JSON_LABEL)
    FROM SCHEMA{report_oid}_LABEL_VIEW l 
    WHERE {}
    GROUP BY l.{}_OID
) || ']'
                                    ",
                                    label_expr_filters.into_iter()
                                        .reduce(|acc, e| format!("{acc} AND {e}"))
                                        .unwrap(),
                                    table_column.datasource_alias
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
                                    "(SELECT GROUP_CONCAT(QF, '&') FROM ({}))",
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
                        let mut expr: FormulaExpression = FormulaExpression::from(trans, formula)?;
                        replace_label_placeholders_in_formula(trans, &mut expr, report_column.child_columns)?;

                        // Value expression
                        c.insert(
                            format!("COLUMN{}_VALUE", report_column.column_metadata.oid),
                            expr.value_expr
                        );
                        // Label expression
                        c.insert(
                            format!("COLUMN{}_LABEL", report_column.column_metadata.oid),
                            format!("COALESCE({}, {})", expr.plain_label_expr, expr.json_label_expr)
                        );
                        // Cell expression
                        c.insert(
                            format!("COLUMN{}_CELL", report_column.column_metadata.oid), 
                            expr.cell_expr
                        );
                        // Isolated reload expression
                        c.insert(
                            format!("COLUMN{}_ISOLATEDRELOAD", report_column.column_metadata.oid),
                            expr.isolated_reload_cells.into_iter()
                                .map(|e| format!("COALESCE({e} || ',', '')"))
                                .reduce(|acc, e| format!("{acc} || {e}"))
                                .unwrap_or(String::from("''"))
                        );
                        // Full reload expression
                        c.insert(
                            format!("COLUMN{}_FULLRELOAD", report_column.column_metadata.oid),
                            expr.full_reload_cells.into_iter()
                                .map(|e| format!("COALESCE({e} || ',', '')"))
                                .reduce(|acc, e| format!("{acc} || {e}"))
                                .unwrap_or(String::from("''"))
                        );
                    }

                    column_type::ColumnType::Subreport { report_oid, .. } => {
                        let mut value_expr_components: Vec<String> = Vec::new();
                        let mut label_expr_filters: Vec<String> = Vec::new();

                        // Collect the filters on the subreport
                        sql_iter(
                            trans,
                            format!("PRAGMA table_info(SCHEMA{report_oid}_LABEL_VIEW)"),
                            [],
                            |row| {
                                let subreport_column_name: String = row.get::<_, String>("name")?;
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
    SELECT 
        GROUP_CONCAT(l.JSON_LABEL)
    FROM SCHEMA{report_oid}_LABEL_VIEW l 
    WHERE {}
    {}
) || ']'
                                    ",
                                    label_expr_filters.into_iter()
                                        .reduce(|acc, e| format!("{acc} AND {e}"))
                                        .unwrap(),
                                    "" // TODO group by for subreports of reports
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
                                    "(GROUP_CONCAT(({}), '&') OVER ({}))",
                                    value_expr_components.into_iter()
                                        .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                        .unwrap(),
                                    "" // TODO partition for subreports of reports
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
    let sql: String = format!(
        "
        CREATE VIEW IF NOT EXISTS SCHEMA{schema_oid}_VIEW AS
        
        WITH {}

        SELECT
            l0.*
            {}
        FROM WRAPPER w
        INNER JOIN SCHEMA{schema_oid}_LABEL_VIEW l0 ON {}
        {}
        ",

        // CTEs
        wrapper.build()?,

        // Index, OIDs, and columns of the schema
        c.iter()
            .map(|(column_alias, column_expression)| format!("{column_expression} AS {column_alias}"))
            .fold(String::from(""), |acc, e| format!("{acc}, {e}")),

        match Datasource::check_default_datasource_transact(trans, schema_oid.clone())? {
            Some(root_datasource) => format!("l0.OID = w.{}_OID", root_datasource.get_alias()),
            None => {
                let filters: Vec<Option<String>> = sql_collect(
                    trans,
                    format!("PRAGMA table_info(SCHEMA{schema_oid}_LABEL_VIEW)"),
                    [],
                    |row| {
                        let column_name: String = row.get::<_, String>("name")?;
                        if column_name.ends_with("_OID") {
                            let datasource: Datasource = {
                                let datasource_alias: String = column_name.replace("_OID", "");
                                Datasource::from_alias_transact(trans, datasource_alias)?
                            };

                            // Check if the OID is present in this view
                            if let Some(datasource_cte) = wrapper.get_datasource_cte(&datasource) {
                                if !datasource_cte.is_always_collection {
                                    // The OID is present in the subreport and in this view, 
                                    // so add the OID as a filter on the subreport
                                    return Ok(Some(format!("l0.{column_name} = w.{}_OID", datasource.get_alias())));
                                }
                            }
                        }
                        Ok(None)
                    }
                )?;
                match filters.into_iter().filter_map(|f| f).reduce(|acc, e| format!("{acc} AND {e}")) {
                    Some(filters) => filters,
                    None => String::from("FALSE")
                }
            }
        },

        // Group by OIDs
        match wrapper.get_oids().into_iter().map(|(_, oid_expr)| oid_expr).reduce(|acc, e| format!("{acc}, {e}")) {
            Some(group_by_expr) => format!("GROUP BY {group_by_expr}"),
            None => String::from("")
        }
    );
    println!("\n{sql}\n");
    sql_execute(trans, sql, [])?;

    Ok(())
}