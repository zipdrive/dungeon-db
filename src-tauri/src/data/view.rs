use crate::{
    data::{column, column_type, datasource::Datasource, schema, table}, util::{error::Error, formula::Formula},
};
use bitflags::bitflags;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::collections::{HashMap, HashSet};


#[derive(Clone)]
struct DatasourceCteColumn {
    /// The expression for the column label.
    label_expr: String,

    /// The ordinal for the column label.
    label_ord: String,

    /// The expression for the column value.
    value_expr: String,

    /// The ordinal for the column value.
    value_ord: String
}

/// A constructor for a CTE that pulls columns from a datasource.
struct DatasourceCteConstructor {
    /// The main datasource.
    datasource: Datasource,

    /// The columns queried in this CTE.
    columns: HashMap<i64, DatasourceCteColumn>,

    /// Datasources that are dependent on this one.
    child_datasources: HashSet<Datasource>
}

impl DatasourceCteConstructor {
    /// Builds the SQL statement for this CTE.
    fn build(&self) -> Result<String, Error> {
        Ok(format!(
            "
            SELECT
                t.OID AS {}_OID
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

            // Columns from this datasource
            self.columns.iter()
                .map(|(_, col)| format!("{} AS {}, {} AS {}", col.label_expr, col.label_ord, col.value_expr, col.value_ord))
                .fold(String::from(""), |acc, e| format!("{acc}, {e}")),
            
            // Parent datasource OID, if applicable
            match &self.datasource {
                Datasource::Table { .. }
                | Datasource::InheritorTable { .. } => String::from(""),
                Datasource::MasterTable { parent_datasource, table_oid } => 
                    format!(", t.MASTER{table_oid}_OID AS PARENT_{}_OID", parent_datasource.get_alias()),
                Datasource::Column { parent_datasource, column } => {
                    match column.column_type {
                        column_type::ColumnType::Object { table_oid, .. }
                        | column_type::ColumnType::Select { table_oid, .. } => {
                            if self.datasource.get_schema_oid()? == column.schema.oid {
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
                        column_type::ColumnType::Multiselect { table_oid, .. } => {
                            format!(
                                ", m.TABLE{}_OID AS PARENT_{}_OID", 
                                parent_datasource.get_schema_oid()?, 
                                parent_datasource.get_alias()
                            )
                        }
                        _ => {
                            return Err(Error::AdhocError("Datasource cannot be derived from a non-Select, non-Object, non-Multiselect column!"));
                        }
                    }
                }
            },

            // Columns from child datasources
            self.child_datasources.iter().map(|child_datasource| child_datasource.get_alias()).fold(String::from(""), |acc, e| format!("{acc}, {e}.*")),

            self.datasource.get_schema_oid()?,

            // Join to multiselect table, if applicable
            match &self.datasource {
                Datasource::Column { column, .. } => {
                    match column.column_type {
                        column_type::ColumnType::Multiselect { .. } => 
                            format!(
                                "INNER JOIN MULTISELECT{} m ON m.TABLE{}_OID = t.OID", 
                                column.oid, 
                                self.datasource.get_schema_oid()?
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
                        Datasource::MasterTable { .. } => {
                            child_datasource_joins = format!(
                                "{child_datasource_joins} INNER JOIN {child_datasource_alias} ON {child_datasource_alias}.PARENT_{}_OID = t.OID",
                                self.datasource.get_alias()
                            );
                        }
                        Datasource::InheritorTable { table_oid, .. } => {
                            child_datasource_joins = format!(
                                "{child_datasource_joins} LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID = t.MASTER{table_oid}_OID"
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
                                    column_type::ColumnType::Object { table_oid, .. }
                                    | column_type::ColumnType::Select { table_oid, .. } => {
                                        if column.schema.oid == self.datasource.get_schema_oid()? {
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
                                        return Err(Error::AdhocError("Datasource cannot be derived from a non-Select, non-Object, non-Multiselect column!"));
                                    }
                                }
                            );
                        }
                        _ => {
                            return Err(Error::AdhocError("Child datasource cannot be a root table!"));
                        }
                    }
                }
                child_datasource_joins
            }
        ))
    }

    /// Adds a primitive column to the CTE.
    /// Assumes that the column is owned by the schema of this datasource.
    fn add_primitive_column(&mut self, column_oid: i64, prim: column_type::Primitive) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            match prim {
                column_type::Primitive::Text
                | column_type::Primitive::JSON => {
                    self.columns.insert(column_oid, DatasourceCteColumn {
                        label_expr: format!("t.COLUMN{column_oid}"),
                        label_ord: format!("{datasource_alias}_COLUMN{column_oid}_LABEL"),
                        value_expr: format!("t.COLUMN{column_oid}"),
                        value_ord: format!("{datasource_alias}_COLUMN{column_oid}_VALUE")
                    });
                }
                column_type::Primitive::Integer
                | column_type::Primitive::Number => {
                    self.columns.insert(column_oid, DatasourceCteColumn {
                        label_expr: format!("CAST(t.COLUMN{column_oid} AS TEXT)"),
                        label_ord: format!("{datasource_alias}_COLUMN{column_oid}_LABEL"),
                        value_expr: format!("t.COLUMN{column_oid}"),
                        value_ord: format!("{datasource_alias}_COLUMN{column_oid}_VALUE")
                    });
                }
                column_type::Primitive::Checkbox => {
                    self.columns.insert(column_oid, DatasourceCteColumn {
                        label_expr: format!("CASE WHEN t.COLUMN{column_oid} IS NULL THEN NULL WHEN t.COLUMN{column_oid} THEN 'true' ELSE 'false' END"),
                        label_ord: format!("{datasource_alias}_COLUMN{column_oid}_LABEL"),
                        value_expr: format!("t.COLUMN{column_oid}"),
                        value_ord: format!("{datasource_alias}_COLUMN{column_oid}_VALUE")
                    });
                }
                column_type::Primitive::Date => {
                    self.columns.insert(column_oid, DatasourceCteColumn {
                        label_expr: format!("DATE(t.COLUMN{column_oid}, 'julianday')"),
                        label_ord: format!("{datasource_alias}_COLUMN{column_oid}_LABEL"),
                        value_expr: format!("t.COLUMN{column_oid}"),
                        value_ord: format!("{datasource_alias}_COLUMN{column_oid}_VALUE")
                    });
                }
                column_type::Primitive::Datetime => {
                    self.columns.insert(column_oid, DatasourceCteColumn {
                        label_expr: format!("STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'julianday')"),
                        label_ord: format!("{datasource_alias}_COLUMN{column_oid}_LABEL"),
                        value_expr: format!("t.COLUMN{column_oid}"),
                        value_ord: format!("{datasource_alias}_COLUMN{column_oid}_VALUE")
                    });
                }
                column_type::Primitive::File
                | column_type::Primitive::Image => {
                    self.columns.insert(column_oid, DatasourceCteColumn {
                        label_expr: format!("(SELECT f.LABEL FROM METADATA_FILE_VIEW f WHERE f.OID = t.COLUMN{column_oid})"),
                        label_ord: format!("{datasource_alias}_COLUMN{column_oid}_LABEL"),
                        value_expr: format!("t.COLUMN{column_oid}"),
                        value_ord: format!("{datasource_alias}_COLUMN{column_oid}_VALUE")
                    });
                }
            }
        }
        return self.columns[&column_oid].clone();
    }

    /// Adds an object column to the CTE.
    fn add_object_column(&mut self, column_oid: i64, object_label_expr: String) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                label_expr: object_label_expr,
                label_ord: format!("{datasource_alias}_COLUMN{column_oid}_LABEL"),
                value_expr: format!("t.COLUMN{column_oid}"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}_VALUE")
            });
        }
        return self.columns[&column_oid].clone();
    }

    /// Adds a select column to the CTE.
    fn add_select_column(&mut self, column_oid: i64, select_label_expr: String) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                label_expr: select_label_expr,
                label_ord: format!("{datasource_alias}_COLUMN{column_oid}_LABEL"),
                value_expr: format!("t.COLUMN{column_oid}"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}_VALUE")
            });
        }
        return self.columns[&column_oid].clone();
    }

    /// Adds a multiselect column to the CTE.
    fn add_multiselect_column(&mut self, column_oid: i64, json_label_expr: String) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                label_expr: format!("NULLIF('[ ' || GROUP_CONCAT(COALESCE({json_label_expr}, '{{}}'), ', ') || ' ]', '[  ]')"),
                label_ord: format!("{datasource_alias}_COLUMN{column_oid}_LABEL"),
                value_expr: format!("GROUP_CONCAT(CAST({datasource_alias}_COLUMN{column_oid}_OID AS TEXT), ',')"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}_VALUE")
            });
        }
        return self.columns[&column_oid].clone();
    }
}


struct SelectParameterType {
    /// The primitive types that the parameter can conform to.
    primitive_types: HashSet<column_type::Primitive>
}

impl SelectParameterType {
    /// 
    fn from(prim: column_type::Primitive) -> Self {
        Self {
            primitive_types: HashSet::from_iter(match &prim {
                column_type::Primitive::Date
                | column_type::Primitive::Datetime => vec![column_type::Primitive::Date, column_type::Primitive::Datetime],
                column_type::Primitive::Text => vec![column_type::Primitive::JSON, prim],
                column_type::Primitive::Number => vec![column_type::Primitive::Integer, prim],
                column_type::Primitive::File => vec![column_type::Primitive::Image, prim],
                _ => vec![prim]
            })
        }
    }

    /// Constructs a type that represents the most specific type that encompasses both this type and the given type.
    fn generalize(&self, other: &Self) -> Self {
        Self {
            primitive_types: HashSet::from_iter(self.primitive_types.union(&(other.primitive_types)).map(|p| p.clone()))
        }
    }

    /// Constructs a type that represents the most general type that conforms to both this type and the given type.
    fn specialize(&self, other: &Self) -> Self {
        Self {
            primitive_types: HashSet::from_iter(self.primitive_types.intersection(&(other.primitive_types)).map(|p| p.clone()))
        }
    }

    /// Returns true if an instance of the given type can always be passed as a value of this type.
    fn encompasses(&self, other: &Self) -> bool {
        self.primitive_types.is_superset(&(other.primitive_types))
    }

    /// Describes the type.
    fn to_string(&self) -> String {
        let mut temp = self.primitive_types.clone();
        if temp.contains(&column_type::Primitive::Datetime) {
            temp.remove(&column_type::Primitive::Date);
        }
        if temp.contains(&column_type::Primitive::Text) {
            temp.remove(&column_type::Primitive::JSON);
        }
        if temp.contains(&column_type::Primitive::Number) {
            temp.remove(&column_type::Primitive::Integer);
        }
        if temp.contains(&column_type::Primitive::File) {
            temp.remove(&column_type::Primitive::Image);
        }
        temp.into_iter()
            .map(|prim| String::from(prim.to_str()))
            .reduce(|acc, e| format!("{acc} | {e}"))
            .unwrap_or(String::from("null"))
    }
}

struct SelectParameter {
    label_expr: String,
    value_expr: String,
    scalar_type: SelectParameterType
}

/// The constructor for a SELECT statement.
struct SelectConstructor {
    /// The CTEs pulling data from a datasource.
    cte_datasource: HashMap<String, DatasourceCteConstructor>
}

impl SelectConstructor {
    /// Builds the SQL syntax for this SELECT statement.
    fn build(&self) -> Result<String, Error> {
        let cte_list: Vec<String> = {
            let mut cte_list: Vec<String> = Vec::new();
            for (cte_name, cte) in self.cte_datasource.iter() {
                cte_list.push(format!("{cte_name} AS ({})", cte.build()?));
            }
            cte_list
        };

        Ok(format!(
            "{}",
            if cte_list.len() > 0 {
                format!("WITH {}", cte_list.join(", "))
            } else {
                String::from("")
            }
        ))
    }

    /// Adds a CTE for a datasource to the SELECT statement.
    fn add_datasource(&mut self, datasource: Datasource) {
        if let Some(parent_datasource) = datasource.get_parent() {
            let parent_datasource_alias: String = parent_datasource.get_alias();
            if !self.cte_datasource.contains_key(&parent_datasource_alias) {
                self.add_datasource(parent_datasource);
            }
            if let Some(parent_datasource_cte) = self.cte_datasource.get_mut(&parent_datasource_alias) {
                parent_datasource_cte.child_datasources.insert(datasource.clone());
            }
        }

        let datasource_alias: String = datasource.get_alias();
        if !self.cte_datasource.contains_key(&datasource_alias) {
            self.cte_datasource.insert(datasource_alias, DatasourceCteConstructor { 
                datasource, 
                columns: HashMap::new(), 
                child_datasources: HashSet::new() 
            });
        }
    }

    /// Adds a column on a datasource as a parameter to this SELECT statement.
    fn add_parameter(&mut self, trans: &Transaction, datasource: Datasource, column: column::FullMetadata) -> Result<SelectParameter, Error> {
        self.add_datasource(datasource.clone());
        match column.column_type {
            column_type::ColumnType::Primitive(prim) => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    let cte_column = cte.add_primitive_column(column.oid, prim.clone());
                    return Ok(SelectParameter {
                        label_expr: cte_column.label_ord,
                        value_expr: cte_column.value_ord,
                        scalar_type: SelectParameterType::from(prim)
                    });
                }
            }
            column_type::ColumnType::Object { table_oid, .. } => {
                let object_label_expr: String = self.construct_object_label_expr(trans, table_oid)?;
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    let cte_column = cte.add_object_column(column.oid, object_label_expr);
                    return Ok(SelectParameter {
                        label_expr: cte_column.label_ord,
                        value_expr: cte_column.value_ord,
                        scalar_type: SelectParameterType {
                            primitive_types: HashSet::new()
                        }
                    });
                }
            }
            column_type::ColumnType::Select { table_oid, .. } => {
                let select_label_expr: String = self.construct_select_label_expr(trans, table_oid)?;
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    let cte_column = cte.add_select_column(column.oid, select_label_expr);
                    return Ok(SelectParameter {
                        label_expr: cte_column.label_ord,
                        value_expr: cte_column.value_ord,
                        scalar_type: SelectParameterType {
                            primitive_types: HashSet::new()
                        }
                    });
                }
            }
            column_type::ColumnType::Multiselect { table_oid, .. } => {
                let json_label_expr: String = self.construct_json_label_expr(trans, table_oid)?;
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    let cte_column = cte.add_multiselect_column(column.oid, json_label_expr);
                    return Ok(SelectParameter {
                        label_expr: cte_column.label_ord,
                        value_expr: cte_column.value_ord,
                        scalar_type: SelectParameterType {
                            primitive_types: HashSet::new()
                        }
                    });
                }
            }
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let parsed_formula: Box<Formula> = Box::new(Formula::parse(formula.clone())?);

                // Compile the formula into SQL
                return self.construct_formula(
                    trans,
                    {
                        if let Datasource::Table { oid, .. } = Datasource::get_default_datasource_transact(trans, datasource.get_schema_oid()?)? {
                            (oid, datasource)
                        } else {
                            return Err(Error::AdhocError("No default datasource for table."));
                        }
                    },
                    parsed_formula
                );
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                let json_label_expr: String = self.construct_json_label_expr(trans, report_oid)?;
                return Ok(SelectParameter {
                    label_expr: json_label_expr.clone(),
                    value_expr: json_label_expr,
                    scalar_type: SelectParameterType {
                        primitive_types: HashSet::new()
                    }
                });
            }
        }
        return Err(Error::AdhocError("Unable to add parameter."));
    }

    /// Construct a label for a Select column.
    fn construct_select_label_expr(&mut self, trans: &Transaction, schema_oid: i64) -> Result<String, Error> {

    }

    /// Construct a JSON label.
    fn construct_json_label_expr(&mut self, trans: &Transaction, schema_oid: i64) -> Result<String, Error> {

    }

    /// Construct a label for an Object column.
    /// This label is in JSON format.
    fn construct_object_label_expr(&mut self, trans: &Transaction, schema_oid: i64) -> Result<String, Error> {

    }

    fn construct_formula(&mut self, trans: &Transaction, root_datasource: (i64, Datasource), formula: Box<Formula>) -> Result<SelectParameter, Error> {
        Ok(match *formula {
            Formula::Abs(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, root_datasource, inner)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("CAST(ABS({}) AS TEXT)", inner_param.value_expr),
                        value_expr: format!("ABS({})", inner_param.value_expr),
                        scalar_type: inner_param.scalar_type
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "ABS(x)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Add(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CAST(({} + {}) AS TEXT)", lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("({} + {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: lhs_param.scalar_type.generalize(&rhs_param.scalar_type)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "_ + rhs", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "lhs + _", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::And(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Checkbox);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CASE WHEN ({} AND {}) IS NULL THEN NULL WHEN ({} AND {}) THEN 'true' ELSE 'false' END", lhs_param.value_expr, rhs_param.value_expr, lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("({} AND {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: SelectParameterType::from(column_type::Primitive::Checkbox)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "_ AND rhs", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "lhs AND _", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Argmax(inners) => {

            }
            Formula::Argmin(inners) => {

            }
            Formula::Average(collection) => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, root_datasource, collection)?;
                if collection_expected_type.encompasses(&collection_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("CAST(AVG({}) AS TEXT)", collection_param.value_expr),
                        value_expr: format!("AVG({})", collection_param.value_expr),
                        scalar_type: collection_param.scalar_type
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "AVERAGE(x)", 
                        inner_name: collection_name,
                        expected_type: collection_expected_type.to_string(), 
                        received_type: collection_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Ceiling(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, root_datasource, inner)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("CAST(CEILING({}) AS TEXT)", inner_param.value_expr),
                        value_expr: format!("CEILING({})", inner_param.value_expr),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Integer)
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "CEILING(x)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }

            }
            Formula::Coalesce(inners) => {

            }
            Formula::Concat(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Text);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CONCAT({}, {})", lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("CONCAT({}, {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: lhs_param.scalar_type.generalize(&rhs_param.scalar_type)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "_ & rhs", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "lhs & _", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Conditional { condition, formula_if_true, formula_if_false } => {
                let condition_expected_type = SelectParameterType::from(column_type::Primitive::Checkbox);
                let condition_name: String = condition.to_string();
                let condition_param = self.construct_formula(trans, root_datasource, condition)?;
                if condition_expected_type.encompasses(&condition_param.scalar_type) {
                    let if_true_param = self.construct_formula(trans, root_datasource, formula_if_true)?;
                    let if_false_param = self.construct_formula(trans, root_datasource, formula_if_false)?;
                    SelectParameter {
                        label_expr: format!("IF({}, {}, {})", condition_param.value_expr, if_true_param.label_expr, if_false_param.label_expr),
                        value_expr: format!("IF({}, {}, {})", condition_param.value_expr, if_true_param.value_expr, if_false_param.value_expr),
                        scalar_type: if_true_param.scalar_type.generalize(&if_false_param.scalar_type)
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "IF(x, _, _)", 
                        inner_name: condition_name,
                        expected_type: condition_expected_type.to_string(), 
                        received_type: condition_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Count(collection) => {
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, root_datasource, collection)?;
                SelectParameter {
                    label_expr: format!("CAST(COUNT({}) AS TEXT)", collection_param.value_expr),
                    value_expr: format!("COUNT({})", collection_param.value_expr),
                    scalar_type: collection_param.scalar_type
                }
            }
            Formula::Divide(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CAST(({} / {}) AS TEXT)", lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("({} / {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: SelectParameterType::from(column_type::Primitive::Number)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "_ / rhs", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "lhs / _", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Eq(lhs, rhs) => {
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                let rhs_name: String = rhs.to_string();
                let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                SelectParameter {
                    label_expr: format!("CASE WHEN ({} IS {}) IS NULL THEN NULL WHEN ({} IS {}) THEN 'true' ELSE 'false' END", lhs_param.value_expr, rhs_param.value_expr, lhs_param.value_expr, rhs_param.value_expr),
                    value_expr: format!("({} IS {})", lhs_param.value_expr, rhs_param.value_expr),
                    scalar_type: SelectParameterType::from(column_type::Primitive::Checkbox)
                }
            }
            Formula::Exponent(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CAST(POW({}, {}) AS TEXT)", lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("POW({}, {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: lhs_param.scalar_type.generalize(&rhs_param.scalar_type)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "POW(_, y)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "POW(x, _)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Floor(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, root_datasource, inner)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("CAST(FLOOR({}) AS TEXT)", inner_param.value_expr),
                        value_expr: format!("FLOOR({})", inner_param.value_expr),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Integer)
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "FLOOR(x)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Format { format, format_params } => {

            }
            Formula::Glob { str, pattern } => {

            }
            Formula::In { value, collection } => {

            }
            Formula::Join { collection, delimiter } => {

            }
            Formula::Length(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Text);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, root_datasource, inner)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("LENGTH({})", inner_param.value_expr),
                        value_expr: format!("LENGTH({})", inner_param.value_expr),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Integer)
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "LENGTH(x)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::LessThan(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CASE WHEN ({} < {}) IS NULL THEN NULL WHEN ({} < {}) THEN 'true' ELSE 'false' END", lhs_param.value_expr, rhs_param.value_expr, lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("({} < {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: SelectParameterType::from(column_type::Primitive::Checkbox)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "_ < rhs", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "lhs < _", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::LessThanOrEq(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CASE WHEN ({} <= {}) IS NULL THEN NULL WHEN ({} <= {}) THEN 'true' ELSE 'false' END", lhs_param.value_expr, rhs_param.value_expr, lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("({} <= {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: SelectParameterType::from(column_type::Primitive::Checkbox)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "_ <= rhs", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "lhs <= _", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::LiteralArray(inners) => {

            }
            Formula::LiteralBool(value) => {
                if value {
                    SelectParameter { 
                        label_expr: String::from("'true'"),
                        value_expr: String::from("TRUE"),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Checkbox)
                    }
                } else {
                    SelectParameter { 
                        label_expr: String::from("'false'"), 
                        value_expr: String::from("FALSE"),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Checkbox)
                    }
                }
            }
            Formula::LiteralFloat(value) => {
                SelectParameter {
                    label_expr: format!("CAST({value} AS TEXT)"),
                    value_expr: format!("{value}"),
                    scalar_type: SelectParameterType::from(column_type::Primitive::Number)
                }
            }
            Formula::LiteralInt(value) => {
                SelectParameter { 
                    label_expr: format!("CAST({value} AS TEXT)"), 
                    value_expr: format!("{value}"),
                    scalar_type: SelectParameterType::from(column_type::Primitive::Integer)
                }
            }
            Formula::LiteralString(value) => {
                let sql_value: String = format!("'{}'", value.replace("'", "''"));
                SelectParameter {
                    label_expr: sql_value.clone(),
                    value_expr: sql_value,
                    scalar_type: SelectParameterType::from(column_type::Primitive::Text)
                }
            }
            Formula::Lowercase(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Text);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, root_datasource, inner)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("LOWER({})", inner_param.value_expr),
                        value_expr: format!("LOWER({})", inner_param.value_expr),
                        scalar_type: inner_param.scalar_type
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "LOWER(x)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Max(collection) => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::Number).generalize(SelectParameterType::from(column_type::Primitive::Text));
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, root_datasource, collection)?;
                if collection_expected_type.encompasses(&collection_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("CAST(MAX({}) AS TEXT)", collection_param.value_expr),
                        value_expr: format!("MAX({})", collection_param.value_expr),
                        scalar_type: collection_param.scalar_type
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "MAX(x)", 
                        inner_name: collection_name,
                        expected_type: collection_expected_type.to_string(), 
                        received_type: collection_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Min(collection) => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::Number).generalize(SelectParameterType::from(column_type::Primitive::Text));
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, root_datasource, collection)?;
                if collection_expected_type.encompasses(&collection_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("CAST(MIN({}) AS TEXT)", collection_param.value_expr),
                        value_expr: format!("MIN({})", collection_param.value_expr),
                        scalar_type: collection_param.scalar_type
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "MIN(x)", 
                        inner_name: collection_name,
                        expected_type: collection_expected_type.to_string(), 
                        received_type: collection_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Modulo(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CAST(({} % {}) AS TEXT)", lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("({} % {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: lhs_param.scalar_type.generalize(&rhs_param.scalar_type)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "_ % rhs", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "lhs % _", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Multiply(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CAST(({} * {}) AS TEXT)", lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("({} * {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: lhs_param.scalar_type.generalize(&rhs_param.scalar_type)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "_ * rhs", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "lhs * _", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Not(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Checkbox);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, root_datasource, inner)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("CASE WHEN {} IS NULL THEN NULL WHEN {} IS FALSE THEN 'true' ELSE 'false' END", inner_param.value_expr, inner_param.value_expr),
                        value_expr: format!("(NOT {})", inner_param.value_expr),
                        scalar_type: inner_param.scalar_type
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "NOT(x)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Null => {
                SelectParameter { 
                    label_expr: String::from("NULL"),
                    value_expr: String::from("NULL"),
                    scalar_type: SelectParameterType {
                        primitive_types: HashSet::new()
                    }
                }
            }
            Formula::NullIf { value, null_if_match } => {
                let lhs_param = self.construct_formula(trans, root_datasource, value)?;
                let rhs_param = self.construct_formula(trans, root_datasource, null_if_match)?;
                SelectParameter {
                    label_expr: format!("CASE WHEN ({} IS {}) THEN NULL ELSE {} END", lhs_param.value_expr, rhs_param.value_expr, lhs_param.label_expr),
                    value_expr: format!("NULLIF({}, {})", lhs_param.value_expr, rhs_param.value_expr),
                    scalar_type: lhs_param.scalar_type
                }
            }
            Formula::Or(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Checkbox);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CASE WHEN ({} OR {}) IS NULL THEN NULL WHEN ({} OR {}) THEN 'true' ELSE 'false' END", lhs_param.value_expr, rhs_param.value_expr, lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("({} OR {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: SelectParameterType::from(column_type::Primitive::Checkbox)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "_ OR rhs", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "lhs OR _", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Param { datasource_alias, column_oid } => {
                let datasource: Datasource = Datasource::from_alias_transact(trans, datasource_alias)?.substitute_root(root_datasource.0, root_datasource.1);
                let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                self.add_parameter(trans, datasource, column)?
            }
            Formula::RandomInt => {

            }
            Formula::Replace { original, pattern, replacement } => {

            }
            Formula::Round(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, root_datasource, inner)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("ROUND({})", inner_param.value_expr),
                        value_expr: format!("ROUND({})", inner_param.value_expr),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Integer)
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "ROUND(x)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Sign(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, root_datasource, inner)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("SIGN({})", inner_param.value_expr),
                        value_expr: format!("SIGN({})", inner_param.value_expr),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Integer)
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "SIGN(x)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Slice { collection, start, length } => {

            }
            Formula::Substring { str, start, length } => {

            }
            Formula::Subtract(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource, lhs)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        SelectParameter {
                            label_expr: format!("CAST(({} - {}) AS TEXT)", lhs_param.value_expr, rhs_param.value_expr),
                            value_expr: format!("({} - {})", lhs_param.value_expr, rhs_param.value_expr),
                            scalar_type: lhs_param.scalar_type.generalize(&rhs_param.scalar_type)
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "_ - rhs", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "lhs - _", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Sum(collection) => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, root_datasource, collection)?;
                if collection_expected_type.encompasses(&collection_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("CAST(SUM({}) AS TEXT)", collection_param.value_expr),
                        value_expr: format!("SUM({})", collection_param.value_expr),
                        scalar_type: collection_param.scalar_type
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "SUM(x)", 
                        inner_name: collection_name,
                        expected_type: collection_expected_type.to_string(), 
                        received_type: collection_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Switch { value, matches, formula_if_no_match } => {

            }
            Formula::Uppercase(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Text);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, root_datasource, inner)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    SelectParameter {
                        label_expr: format!("UPPER({})", inner_param.value_expr),
                        value_expr: format!("UPPER({})", inner_param.value_expr),
                        scalar_type: inner_param.scalar_type
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "UPPER(x)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Wrap(inner) => {
                self.construct_formula(trans, root_datasource, inner)?
            }
        })
    }
}




struct ViewsToCreate {
    /// The OID of the schema to create views for.
    schema_oid: i64,

    /// True if the label view needs to be created. False otherwise.
    create_label_view: bool,

    /// True if the polymorphism view needs to be created. False otherwise.
    create_polymorphism_view: bool,
}

/// Drop the views associated with a schema.
fn drop_all_views(
    trans: &Transaction,
    schema_oid: i64,
    create_schema_oid_seq: &mut Vec<ViewsToCreate>,
) -> Result<(), Error> {
    if create_schema_oid_seq
        .iter()
        .any(|view_to_create| view_to_create.schema_oid == schema_oid)
    {
        // Prevent possible infinite recursions
        return Ok(());
    }
    create_schema_oid_seq.push(ViewsToCreate {
        schema_oid,
        create_label_view: true,
        create_polymorphism_view: true,
    });

    // Drop the views associated with any master schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    for row_result in trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE INHERITOR_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("MASTER_SCHEMA_OID"))? {
        let master_schema_oid: i64 = row_result?;
        drop_all_views(trans, master_schema_oid, create_schema_oid_seq)?;
    }

    // Drop the views associated with any inheritor schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
        let inheritor_schema_oid: i64 = row_result?;
        drop_all_views(trans, inheritor_schema_oid, create_schema_oid_seq)?;
    }

    // Drop the views that use the label
    for row_result in trans
        .prepare(
            "
        SELECT 
            c.SCHEMA_OID, 
            c.IS_PRIMARY_KEY 
        FROM METADATA_COLUMN c 
        INNER JOIN METADATA_COLUMN_TYPE__OBJECT o ON o.OID = c.TYPE_OID
        WHERE o.TABLE_OID = ?1

        UNION ALL 

        SELECT 
            c.SCHEMA_OID, 
            c.IS_PRIMARY_KEY 
        FROM METADATA_COLUMN c 
        INNER JOIN METADATA_COLUMN_TYPE__SELECT s ON s.OID = c.TYPE_OID
        WHERE s.TABLE_OID = ?1

        UNION ALL 

        SELECT 
            c.SCHEMA_OID, 
            c.IS_PRIMARY_KEY 
        FROM METADATA_COLUMN c 
        INNER JOIN METADATA_COLUMN_TYPE__MULTISELECT s ON s.OID = c.TYPE_OID
        WHERE s.TABLE_OID = ?1
        ",
        )?
        .query_map(params![schema_oid], |row| {
            Ok((
                row.get::<_, i64>("SCHEMA_OID")?,
                row.get::<_, bool>("IS_PRIMARY_KEY")?,
            ))
        })?
    {
        let (referencing_schema_oid, referenced_in_label) = row_result?;
        drop_views_associated_with_label(
            trans,
            referencing_schema_oid,
            referenced_in_label,
            create_schema_oid_seq,
        )?;
    }

    // Drop the associated views
    let drop_sql: String = format!(
        "
        DROP VIEW IF EXISTS SCHEMA{schema_oid}_VIEW;
        DROP VIEW IF EXISTS TABLE{schema_oid}_LABEL_VIEW;
        DROP VIEW IF EXISTS TABLE{schema_oid}_POLYMORPHISM_VIEW;
    "
    );
    trans.execute_batch(&drop_sql)?;
    Ok(())
}

/// Drop the views that reference the label of another schema.
fn drop_views_associated_with_label(
    trans: &Transaction,
    schema_oid: i64,
    drop_label_view: bool,
    create_schema_oid_seq: &mut Vec<ViewsToCreate>,
) -> Result<(), Error> {
    if create_schema_oid_seq
        .iter()
        .any(|view_to_create| view_to_create.schema_oid == schema_oid)
    {
        // Prevent possible infinite recursions
        return Ok(());
    }
    create_schema_oid_seq.push(ViewsToCreate {
        schema_oid,
        create_label_view: drop_label_view.clone(),
        create_polymorphism_view: false,
    });

    // Drop the primary schema view
    let drop_main_view_sql: String = format!("DROP VIEW IF EXISTS SCHEMA{schema_oid}_VIEW");
    trans.execute(&drop_main_view_sql, [])?;

    if drop_label_view {
        // Drop the views associated with any inheritor schema
        // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
        for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
            let inheritor_schema_oid: i64 = row_result?;
            drop_views_associated_with_label(trans, inheritor_schema_oid, true, create_schema_oid_seq)?;
        }

        // Drop all views that require that label view
        for row_result in trans
            .prepare(
                "
            SELECT 
                c.SCHEMA_OID, 
                c.IS_PRIMARY_KEY 
            FROM METADATA_COLUMN c 
            INNER JOIN METADATA_COLUMN_TYPE__OBJECT o ON o.OID = c.TYPE_OID
            WHERE o.TABLE_OID = ?1

            UNION ALL 

            SELECT 
                c.SCHEMA_OID, 
                c.IS_PRIMARY_KEY 
            FROM METADATA_COLUMN c 
            INNER JOIN METADATA_COLUMN_TYPE__SELECT s ON s.OID = c.TYPE_OID
            WHERE s.TABLE_OID = ?1

            UNION ALL 

            SELECT 
                c.SCHEMA_OID, 
                c.IS_PRIMARY_KEY 
            FROM METADATA_COLUMN c 
            INNER JOIN METADATA_COLUMN_TYPE__MULTISELECT s ON s.OID = c.TYPE_OID
            WHERE s.TABLE_OID = ?1
            ",
            )?
            .query_map(params![schema_oid], |row| {
                Ok((
                    row.get::<_, i64>("SCHEMA_OID")?,
                    row.get::<_, bool>("IS_PRIMARY_KEY")?,
                ))
            })?
        {
            let (referencing_schema_oid, referenced_in_label) = row_result?;
            drop_views_associated_with_label(
                trans,
                referencing_schema_oid,
                referenced_in_label,
                create_schema_oid_seq,
            )?;
        }

        // Drop the label view
        let drop_label_view_sql: String =
            format!("DROP VIEW IF EXISTS TABLE{schema_oid}_LABEL_VIEW");
        trans.execute(&drop_label_view_sql, [])?;
    }
    Ok(())
}

/// Compiles a CTE to determine the lowest-level inheritor table that is associated with a particular row in the master table.
fn compile_polymorphism_cte(
    trans: &Transaction,
    table_oid: i64,
    compiled_cte: &mut HashMap<String, String>,
) -> Result<(), Error> {
    let cte_name: String = format!("TABLE{table_oid}_POLYMORPHISM_CTE");
    if compiled_cte.contains_key(&cte_name) {
        return Ok(()); // Prevent infinite recursion, just in case
    }

    // The components of the query will be combined with UNION
    let mut polymorphism_cte_components: Vec<String> = Vec::new();

    // Get polymorphism of inheritor tables
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![table_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
        // Ensure that inheritor CTE is compiled
        let inheritor_table_oid: i64 = row_result?;
        compile_polymorphism_cte(trans, inheritor_table_oid, compiled_cte)?;

        // Get the polymorphism from the inheritor CTE
        polymorphism_cte_components.push(format!(
            "
            SELECT
                i.MASTER{table_oid}_OID AS OID,
                p.TABLE_OID,
                p.ROW_OID
            FROM TABLE{inheritor_table_oid}_POLYMORPHISM_CTE p
            INNER JOIN TABLE{inheritor_table_oid} i ON i.OID = p.OID
            "
        ));
    }

    // Compile the final CTE
    compiled_cte.insert(
        cte_name,
        if polymorphism_cte_components.len() > 0 {
            format!(
                "
                SELECT
                    t.OID,
                    COALESCE(u.TABLE_OID, {table_oid}) AS TABLE_OID,
                    COALESCE(u.ROW_OID, t.OID) AS ROW_OID
                FROM TABLE{table_oid} t
                LEFT JOIN ({}) u ON u.OID = t.OID
                WHERE NOT t.TRASH
                ",
                polymorphism_cte_components
                    .into_iter()
                    .reduce(|acc, e| format!("{acc} UNION {e}"))
                    .unwrap()
            )
        } else {
            format!(
                "
                SELECT
                    t.OID,
                    {table_oid} AS TABLE_OID,
                    t.OID AS ROW_OID
                FROM TABLE{table_oid} t
                WHERE NOT t.TRASH
                "
            )
        },
    );
    Ok(())
}

/// Create a view describing the lowest-level table that has a row inheriting from a particular row in the table.
fn create_table_polymorphism_view(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    println!("Creating TABLE{table_oid}_POLYMORPHISM_VIEW...");

    let final_cte_name: String = format!("TABLE{table_oid}_POLYMORPHISM_CTE");
    let view_name: String = format!("TABLE{table_oid}_POLYMORPHISM_VIEW");

    // Compile all necessary CTEs
    let mut compiled_cte: HashMap<String, String> = HashMap::new();
    compile_polymorphism_cte(trans, table_oid.clone(), &mut compiled_cte)?;

    // Compile and create the final view
    if let Some(final_cte) = compiled_cte.remove(&final_cte_name) {
        let create_sql: String = format!(
            "
            CREATE VIEW IF NOT EXISTS {view_name} AS 
            {}
            {final_cte}
            ",
            if compiled_cte.len() > 0 {
                format!(
                    "WITH {}",
                    compiled_cte
                        .into_iter()
                        .map(|(cte_name, cte_sql)| format!("{cte_name} AS ({cte_sql})"))
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap()
                )
            } else {
                String::from("")
            }
        );
        println!("{create_sql}");
        trans.execute(&create_sql, [])?;
    }
    Ok(())
}

/// Compiles a CTE to get the primary key columns for a particular row in a table.
fn compile_keycolumn_cte(
    trans: &Transaction,
    table_oid: i64,
    compiled_cte: &mut HashMap<String, String>,
) -> Result<bool, Error> {
    // Prevent duplication
    let cte_name: String = format!("TABLE{table_oid}_KEYCOLUMNS_CTE");
    if compiled_cte.contains_key(&cte_name) {
        return Ok(compiled_cte[&cte_name] != "...");
    }
    compiled_cte.insert(cte_name.clone(), String::from("..."));

    // The components of the query will be combined with UNION
    let mut column_cte_components: Vec<String> = Vec::new();

    // Get primary keys of master tables
    for row_result in trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE INHERITOR_SCHEMA_OID = ?1")?.query_map(params![table_oid], |row| row.get::<_, i64>("MASTER_SCHEMA_OID"))? {
        // Ensure that master CTE is compiled
        let master_table_oid: i64 = row_result?;
        if compile_keycolumn_cte(trans, master_table_oid, compiled_cte)? {
            // Get the columns from the master CTE
            column_cte_components.push(format!(
                "
                SELECT
                    t.OID,
                    m.PLAIN_LABEL,
                    m.JSON_LABEL
                FROM TABLE{master_table_oid}_KEYCOLUMNS_CTE m
                INNER JOIN TABLE{table_oid} t ON t.MASTER{master_table_oid}_OID = m.OID
                "
            ));
        } // Otherwise, ignore the primary keys of the master table
    }

    // Get each primary key column from this table
    for row_result in trans.prepare("SELECT OID FROM METADATA_COLUMN WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY AND NOT TRASH")?.query_map(params![table_oid], |row| row.get::<_, i64>("OID"))? {
        let column_oid: i64 = row_result?;
        let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
        let sanitized_column_name: String = column_metadata.name.replace("\\", "\\\\").replace("\"", "\\\"").replace("'", "''");

        // Determine expression for this specific column
        match column_metadata.column_type {
            column_type::ColumnType::Primitive(prim) => {
                match prim {
                    column_type::Primitive::Integer
                    | column_type::Primitive::Number 
                    | column_type::Primitive::JSON => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                t.COLUMN{column_oid} AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE(CAST(t.COLUMN{column_oid} AS TEXT), 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Text => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                t.COLUMN{column_oid} AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || REPLACE(REPLACE(t.COLUMN{column_oid}, '\\', '\\\\'), '\"', '\\\"') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Checkbox => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                IF(t.COLUMN{column_oid}, 'True', 'False') AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || CASE WHEN t.COLUMN{column_oid} IS NULL THEN 'null' WHEN t.COLUMN{column_oid} THEN 'true' ELSE 'false' END AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Date => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                DATE(t.COLUMN{column_oid}, 'julianday') AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || DATE(t.COLUMN{column_oid}, 'julianday') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Datetime => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'julianday') AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'julianday') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::File 
                    | column_type::Primitive::Image => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                f.LABEL AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || REPLACE(REPLACE(f.LABEL, '\\', '\\\\'), '\"', '\\\"') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            LEFT JOIN METADATA_FILE_VIEW f ON f.OID = t.COLUMN{column_oid}
                            "
                        ));
                    }
                }
            }
            column_type::ColumnType::Object { table_oid: object_table_oid, .. } => {
                if compile_label_cte(trans, object_table_oid, compiled_cte)? {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            o.PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ' || COALESCE('\"' || REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') || '\": {{' || s.JSON_LABEL || '}}', 'null') AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        LEFT JOIN TABLE{object_table_oid}_LABEL_CTE o ON o.OID = t.COLUMN{column_oid}
                        LEFT JOIN METADATA_SCHEMA s ON s.OID = o.SCHEMA_OID
                        "
                    ));
                } else {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            '...' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ...' AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        "
                    ));
                }
            }
            column_type::ColumnType::Select { table_oid: select_table_oid, .. } => {
                if compile_label_cte(trans, select_table_oid, compiled_cte)? {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            s.PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ' || COALESCE(s.JSON_LABEL, 'null') AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        LEFT JOIN TABLE{select_table_oid}_LABEL_CTE s ON s.OID = t.COLUMN{column_oid}
                        "
                    ));
                } else {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            '...' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ...' AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        "
                    ));
                }
            }
            column_type::ColumnType::Multiselect { table_oid: select_table_oid, .. } => {
                if compile_label_cte(trans, select_table_oid, compiled_cte)? {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            m.TABLE{table_oid}_OID AS OID,
                            '[' || GROUP_CONCAT('\"' || REPLACE(REPLACE(s.PLAIN_LABEL, '\\', '\\\\'), '\"', '\\\"') || '\"', ', ') || ']' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": [' || COALESCE(GROUP_CONCAT(s.JSON_LABEL, ', '), '') || ']' AS JSON_LABEL
                        FROM MULTISELECT{column_oid} m
                        LEFT JOIN TABLE{select_table_oid}_LABEL_CTE s ON s.OID = m.TABLE{select_table_oid}_OID
                        GROUP BY m.TABLE{table_oid}_OID
                        "
                    ));
                } else {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            '[...]' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": [...]' AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        "
                    ));
                }
            }
            _ => {
                // Skip any other type of column
            }
        }
    }

    // Compile the final CTE
    match column_cte_components
        .into_iter()
        .reduce(|acc, e| format!("{acc} UNION {e}"))
    {
        Some(compiled_column_cte) => {
            compiled_cte.insert(cte_name, compiled_column_cte);
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Compiles a CTE to get the label for a particular row in a table.
fn compile_label_cte(
    trans: &Transaction,
    table_oid: i64,
    compiled_cte: &mut HashMap<String, String>,
) -> Result<bool, Error> {
    let cte_name: String = format!("TABLE{table_oid}_LABEL_CTE");
    if compiled_cte.contains_key(&cte_name) {
        return Ok(compiled_cte[&cte_name] == "...");
    }
    compiled_cte.insert(cte_name.clone(), String::from("..."));

    // Ensure that the polymorphism CTE has been compiled
    compile_polymorphism_cte(trans, table_oid, compiled_cte)?;

    // Ensure that the keycolumns CTE has been compiled
    if !compile_keycolumn_cte(trans, table_oid, compiled_cte)? {
        // Again, something is completely fucked, recursion-wise
        return Ok(false);
    }

    let mut label_cte_components: Vec<String> = Vec::new();

    // Get labels of inheritor tables
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![table_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
        // Ensure that inheritor CTE is compiled
        let inheritor_table_oid: i64 = row_result?;
        if compile_label_cte(trans, inheritor_table_oid, compiled_cte)? {
            label_cte_components.push(format!(
                "
                SELECT
                    i.MASTER{table_oid}_OID AS OID,
                    lbl.TABLE_OID,
                    lbl.ROW_OID,
                    lbl.PLAIN_LABEL,
                    lbl.JSON_LABEL
                FROM TABLE{inheritor_table_oid}_LABEL_CTE lbl
                INNER JOIN TABLE{inheritor_table_oid} i ON i.OID = lbl.OID
                "
            ));
        } // Otherwise, ignore the inheritor labels
    }

    if label_cte_components.len() > 0 {
        compiled_cte.insert(
            cte_name,
            format!(
                "
            SELECT
                p.OID,
                p.TABLE_OID,
                p.ROW_OID,
                COALESCE(u.PLAIN_LABEL,
                    (
                        SELECT
                            CASE
                                WHEN COUNT(k.JSON_LABEL) = 0 THEN '— NO PRIMARY KEY —'
                                WHEN COUNT(k.JSON_LABEL) = 1 THEN MIN(k.PLAIN_LABEL)
                                ELSE NULL
                            END
                        FROM TABLE{table_oid}_KEYCOLUMNS_CTE k 
                        WHERE k.OID = p.OID
                        GROUP BY p.OID
                    )
                ) AS PLAIN_LABEL,
                COALESCE(u.JSON_LABEL, 
                    (
                        SELECT 
                            '{{' || COALESCE(GROUP_CONCAT(k.JSON_LABEL, ', '), '') || '}}' 
                        FROM TABLE{table_oid}_KEYCOLUMNS_CTE k 
                        WHERE k.OID = p.OID
                        GROUP BY p.OID
                    )
                ) AS JSON_LABEL
            FROM TABLE{table_oid}_POLYMORPHISM_CTE p
            LEFT JOIN ({}) u ON u.TABLE_OID = p.TABLE_OID AND u.ROW_OID = p.ROW_OID
            ",
                label_cte_components
                    .into_iter()
                    .reduce(|acc, e| format!("{acc} UNION {e}"))
                    .unwrap()
            ),
        );
    } else {
        compiled_cte.insert(
            cte_name,
            format!(
                "
            SELECT
                p.OID,
                p.TABLE_OID,
                p.ROW_OID,
                CASE
                    WHEN COUNT(k.JSON_LABEL) = 0 THEN '— NO PRIMARY KEY —'
                    WHEN COUNT(k.JSON_LABEL) = 1 THEN MIN(k.PLAIN_LABEL)
                    ELSE NULL
                END AS PLAIN_LABEL,
                '{{' || COALESCE(GROUP_CONCAT(k.JSON_LABEL, ', '), '') || '}}' AS JSON_LABEL
            FROM TABLE{table_oid}_POLYMORPHISM_CTE p
            INNER JOIN TABLE{table_oid}_KEYCOLUMNS_CTE k ON k.OID = p.OID
            GROUP BY
                p.OID,
                p.TABLE_OID,
                p.ROW_OID
            "
            ),
        );
    }
    Ok(true)
}

/// Create a view for the label of each row in the table.
fn create_table_label_view(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    println!("Creating TABLE{table_oid}_LABEL_VIEW...");

    let mut compiled_cte: HashMap<String, String> = HashMap::new();
    let create_sql: String = if compile_label_cte(trans, table_oid, &mut compiled_cte)? {
        format!(
            "
            CREATE VIEW IF NOT EXISTS TABLE{table_oid}_LABEL_VIEW AS
            WITH {} 
            SELECT
                lbl.OID,
                lbl.TABLE_OID,
                lbl.ROW_OID,
                COALESCE(lbl.PLAIN_LABEL, lbl.JSON_LABEL) AS SELECT_LABEL,
                lbl.JSON_LABEL,
                '{{\"' || REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') || '\": ' || lbl.JSON_LABEL || '}}' AS OBJECT_LABEL
            FROM TABLE{table_oid}_LABEL_CTE lbl
            INNER JOIN METADATA_SCHEMA s ON s.OID = lbl.TABLE_OID
            ",
            compiled_cte.into_iter().map(|(cte_name, cte_definition)| format!("{cte_name} AS ({cte_definition})")).reduce(|acc, e| format!("{acc}, {e}")).unwrap_or(format!("
                TABLE{table_oid}_LABEL_CTE AS (
                    SELECT
                        OID,
                        TABLE_OID,
                        ROW_OID,
                        '...' AS SELECT_LABEL,
                        '{{ ... }}' AS JSON_LABEL
                    FROM TABLE{table_oid}_POLYMORPHISM_VIEW p 
                )
            "))
        )
    } else {
        // Create a label view with dummy labels
        format!(
            "
            CREATE VIEW IF NOT EXISTS TABLE{table_oid}_LABEL_VIEW AS 
            SELECT
                p.OID,
                p.TABLE_OID,
                p.ROW_OID,
                '...' AS SELECT_LABEL,
                '{{ ... }}' AS JSON_LABEL,
                '{{\"' || REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') || '\": {{ ... }}}}' AS OBJECT_LABEL
            FROM TABLE{table_oid}_POLYMORPHISM_VIEW p 
            INNER JOIN METADATA_SCHEMA s ON s.OID = p.TABLE_OID
            "
        )
    };
    println!("{create_sql}");
    trans.execute(&create_sql, [])?;
    Ok(())
}

enum SchemaViewColumn {
    TableData {
        label_expr: String,
        label_ord: String,
        value_expr: String,
        value_ord: String,
    },
    Formula {
        label_expr: String,
        label_ord: String,
        value_expr: String,
        value_ord: String,
        param_expr: String,
        param_ord: String,
    },
    Subreport {
        label_expr: String,
        label_ord: String
    }
}

impl SchemaViewColumn {
    /// Compiles the column.
    pub fn compile(&self) -> Result<String, Error> {
        Ok(match self {
            Self::TableData {
                label_expr,
                label_ord,
                value_expr,
                value_ord,
            } => format!("{label_expr} AS {label_ord}, {value_expr} AS {value_ord}"),
            Self::Formula {
                label_expr,
                label_ord,
                value_expr,
                value_ord,
                param_expr,
                param_ord,
            } => {
                format!("{label_expr} AS {label_ord}, {value_expr} AS {value_ord}, {param_expr} AS {param_ord}")
            }
            Self::Subreport {
                label_expr,
                label_ord 
            } => {
                format!("{label_expr} AS {label_ord}")
            }
        })
    }
}

#[derive(PartialEq, Eq, Clone)]
struct PrimitiveScalarType(u32);
bitflags! {
    impl PrimitiveScalarType: u32 {
        const Null          = 0b00000000;
        const AnyPrimitive  = 0b11111111;
        const Boolean       = 0b00000001;
        const Integer       = 0b00000010;
        const Number        = 0b00000110;
        const Date          = 0b00001000;
        const Datetime      = 0b00011000;
        const Text          = 0b00100000;
        const JSON          = 0b01000000;

        /// File data is represented as an OID in the METADATA_FILE table.
        const File          = 0b10000000;
    }
}

impl PrimitiveScalarType {
    /// Converts from a scalar type to a string.
    fn to_string(&self) -> String {
        let mut flags: Vec<Self> = self.iter().collect();
        // Reduce flags to minimal set
        let mut k: usize = 0;
        while k < flags.len() {
            // Iterate over each other flag, testing if this flag is contained in the other
            let mut j: usize = 0;
            while j < flags.len() {
                if j != k && flags[j].contains(flags[k].clone()) {
                    flags.remove(k.clone());
                    k -= 1; // Decrement to negate the increment
                    break;
                }
                // Increment the index being compared to
                j += 1;
            }

            // Increment index
            k += 1;
        }
        // Concatenate different types together
        flags
            .into_iter()
            .map(|flag| match flag {
                Self::Null => String::from("null"),
                Self::AnyPrimitive => String::from("primitive"),
                Self::Boolean => String::from("boolean"),
                Self::Integer => String::from("integer"),
                Self::Number => String::from("number"),
                Self::Date => String::from("date"),
                Self::Datetime => String::from("timestamp"),
                Self::Text => String::from("text"),
                Self::JSON => String::from("JSON"),
                Self::File => String::from("file"),
                _ => String::from("unknown"), // This case shouldn't ever happen; if it does, something has gone wrong
            })
            .reduce(|acc, e| format!("{acc} | {e}"))
            .unwrap_or(String::from("null"))
    }
}

#[derive(PartialEq, Eq, Clone)]
enum ExpressionReturnType {
    // Any possible value or reference.
    Any,

    // A more specific type.
    Selected {
        // The type can take on a primitive value.
        primitive: PrimitiveScalarType,

        // The type can take on a reference to a record in one of the indicated tables.
        record_in_table_oid: HashSet<i64>,
    },
}

impl ExpressionReturnType {
    /// Construct a new type representing a primitive value.
    pub fn new_primitive(primitive: PrimitiveScalarType) -> Self {
        Self::Selected {
            primitive,
            record_in_table_oid: HashSet::new(),
        }
    }

    /// Construct a new type representing a reference to a record in the table with the provided OID.
    pub fn new_record(table_oid: i64) -> Self {
        let mut record_in_table_oid: HashSet<i64> = HashSet::new();
        record_in_table_oid.insert(table_oid);
        Self::Selected {
            primitive: PrimitiveScalarType::Null,
            record_in_table_oid,
        }
    }

    /// Returns true if a parameter of this type can accept an argument of the given type.
    /// In other words, returns true if this type is equivalent to or a supertype of the given type.
    pub fn accepts_arg(&self, other: &ExpressionReturnType) -> bool {
        match self {
            Self::Any => true,
            Self::Selected {
                primitive: self_primitive,
                record_in_table_oid: self_table_oid,
            } => match other {
                Self::Any => false,
                Self::Selected {
                    primitive: other_primitive,
                    record_in_table_oid: other_table_oid,
                } => {
                    self_primitive.contains(other_primitive.clone())
                        && self_table_oid.is_superset(other_table_oid)
                }
            },
        }
    }

    /// Returns a type that encompasses both this type and the given type.
    pub fn generalize(&self, other: &ExpressionReturnType) -> Self {
        match self {
            Self::Any => Self::Any,
            Self::Selected {
                primitive: self_primitive,
                record_in_table_oid: self_table_oid,
            } => match other {
                Self::Any => Self::Any,
                Self::Selected {
                    primitive: other_primitive,
                    record_in_table_oid: other_table_oid,
                } => Self::Selected {
                    primitive: self_primitive.clone() | other_primitive.clone(),
                    record_in_table_oid: self_table_oid
                        .union(other_table_oid)
                        .map(|ir| ir.clone())
                        .collect(),
                },
            },
        }
    }

    /// Returns a type that is encompassed by both this type and the given type.
    pub fn specialize(&self, other: &ExpressionReturnType) -> Self {
        match self {
            Self::Any => other.clone(),
            Self::Selected {
                primitive: self_primitive,
                record_in_table_oid: self_table_oid,
            } => match other {
                Self::Any => Self::Selected {
                    primitive: self_primitive.clone(),
                    record_in_table_oid: self_table_oid.clone(),
                },
                Self::Selected {
                    primitive: other_primitive,
                    record_in_table_oid: other_table_oid,
                } => Self::Selected {
                    primitive: self_primitive.clone() & other_primitive.clone(),
                    record_in_table_oid: self_table_oid
                        .intersection(other_table_oid)
                        .map(|ir| ir.clone())
                        .collect(),
                },
            },
        }
    }

    /// Converts the expression return type to a string.
    pub fn to_string(&self, conn: &Connection) -> String {
        match self {
            Self::Any => format!("any"),
            Self::Selected {
                primitive,
                record_in_table_oid,
            } => {
                let mut record_types: Vec<String> = Vec::new();
                if record_in_table_oid.len() > 0 {
                    for table_oid in record_in_table_oid {
                        if let Ok(schema_metadata) =
                            schema::FullMetadata::get(conn, table_oid.clone())
                        {
                            record_types.push(format!("record [{}]", schema_metadata.name));
                        } else {
                            record_types.push(String::from("record [-ERROR-]"));
                        }
                    }

                    if primitive == &PrimitiveScalarType::Null {
                        record_types
                            .into_iter()
                            .reduce(|acc, e| format!("{acc} | {e}"))
                            .unwrap_or(String::from("null"))
                    } else {
                        record_types
                            .into_iter()
                            .fold(primitive.to_string(), |acc, e| format!("{acc} | {e}"))
                    }
                } else {
                    primitive.to_string()
                }
            }
        }
    }
}

#[derive(Clone)]
struct ParamCTEColumnCell {
    /// The OID of the table. May be different from the schema OID indicated by the column, if the cell has a reversed relationship.
    table_oid: i64,

    /// The OID of the column.
    column_oid: i64,

    /// The ordinal of the row OID.
    row_ord: String,
}

#[derive(Clone)]
struct ParamCTEColumn {
    /// The expression for the label.
    /// Always returns a string that is 1-to-1 with its datasource.
    label_expr: String,

    /// The ordinal of the label.
    label_ord: String,

    /// The expression for the value.
    /// Always returns a value that is 1-to-1 with its datasource.
    value_expr: String,

    /// The ordinal of the value.
    value_ord: String,

    /// The expression for the parameter as an argument to a formula.
    /// May return a value that is 1-to-* with its datasource, in the case of reversed Object columns, reversed Select columns, or normal/reversed Multiselect columns.
    arg_expr: String,

    /// The type of the parameter as an argument to a formula.
    arg_type: ExpressionReturnType,

    /// The identifier for the column and row.
    cell: Option<ParamCTEColumnCell>,
}

struct ParamCTE {
    datasource: Datasource,
    child_datasources: HashSet<Datasource>,
    columns: HashMap<String, ParamCTEColumn>,
    is_grouped: bool
}

impl ParamCTE {
    /// Compiles the CTE.
    pub fn compile(self) -> Result<String, Error> {
        let datasource_alias: String = self.datasource.get_alias();
        let datasource_schema_oid: i64 = self.datasource.get_schema_oid()?;

        // If datasource is an Object or Select column with a reversed relationship, make sure that column is included in the CTE
        // If datasource is an inheritor table, make sure the OID of the master table is included in the CTE
        let key: String = match &self.datasource {
            Datasource::InheritorTable {
                parent_datasource, ..
            } => format!(
                ", d.MASTER{}_OID AS {datasource_alias}_KEY",
                parent_datasource.get_schema_oid()?
            ),
            Datasource::Column { column, .. } => {
                match column.column_type {
                    column_type::ColumnType::Object { table_oid, .. }
                    | column_type::ColumnType::Select { table_oid, .. } => {
                        if column.schema.oid == datasource_schema_oid {
                            // Relationship is reversed
                            format!(", d.COLUMN{} AS {datasource_alias}_KEY", column.oid)
                        } else {
                            String::from("")
                        }
                    }
                    _ => String::from(""),
                }
            }
            _ => String::from(""),
        };

        // Select the columns for OIDs/parameters from child datasources and the FROM/JOIN tables/CTEs
        let mut oid_columns_and_datasources_raw: Vec<(String, String)> = Vec::new();
        for child_datasource in self.child_datasources.into_iter() {
            let child_datasource_alias: String = child_datasource.get_alias();
            oid_columns_and_datasources_raw.push((
                format!("{child_datasource_alias}.*"),
                match &child_datasource {
                    Datasource::Table { .. } => {
                        // This case shouldn't happen
                        format!("INNER JOIN {child_datasource_alias}")
                    }
                    Datasource::MasterTable { table_oid, .. } => {
                        format!("INNER JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID = d.MASTER{table_oid}_OID")
                    }
                    Datasource::InheritorTable { .. } => {
                        format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_KEY = d.OID")
                    }
                    Datasource::Column { column, .. } => {
                        match column.column_type {
                            column_type::ColumnType::Object { table_oid, .. }
                            | column_type::ColumnType::Select { table_oid, .. } => {
                                if column.schema.oid == datasource_schema_oid {
                                    // Is normal
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID = d.COLUMN{}", column.oid)
                                } else if table_oid == datasource_schema_oid {
                                    // Is reversed
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_KEY = d.OID")
                                } else {
                                    // Invalid relationship
                                    format!("LEFT JOIN {child_datasource_alias} ON FALSE")
                                }
                            }
                            column_type::ColumnType::Multiselect { table_oid, .. } => {
                                if column.schema.oid == datasource_schema_oid {
                                    // Is normal
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID IN (SELECT m.TABLE{table_oid}_OID FROM MULTISELECT{} m WHERE m.TABLE{}_OID = d.OID)", column.oid, column.schema.oid)
                                } else if table_oid == datasource_schema_oid {
                                    // Is reversed
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID IN (SELECT m.TABLE{}_OID FROM MULTISELECT{} m WHERE m.TABLE{table_oid}_OID = d.OID)", column.schema.oid, column.oid)
                                } else {
                                    // Invalid relationship
                                    format!("LEFT JOIN {child_datasource_alias} ON FALSE")
                                }
                            }
                            _ => {
                                return Err(Error::InvalidDatasourceColumn { 
                                    column_oid: column.oid.clone(), 
                                    column_name: column.name.clone(), 
                                    column_type: column.column_type.to_str()
                                });
                            }
                        }
                    }
                }
            ));
        }
        let (oid_columns, datasources) = oid_columns_and_datasources_raw.into_iter().fold(
            (
                format!("d.OID AS {datasource_alias}_OID{key}"),
                format!("FROM TABLE{datasource_schema_oid} d"),
            ),
            |(acc1, acc2), (e1, e2)| (format!("{acc1}, {e1}"), format!("{acc2} {e2}")),
        );

        // Compile all columns
        let all_columns: String = self
            .columns
            .into_iter()
            .map(|(_, column)| {
                format!(
                    "{} AS {}, {} AS {}",
                    column.label_expr, column.label_ord, column.value_expr, column.value_ord
                )
            })
            .fold(oid_columns, |acc, e| format!("{acc}, {e}"));

        Ok(format!(
            "
            {datasource_alias} AS (
                SELECT 
                    {all_columns} 
                {datasources}
                WHERE NOT d.TRASH
            )
            "
        ))
    }
}

/// Represents an expression returning a scalar value.
#[derive(PartialEq, Eq, Clone)]
struct ScalarExpression {
    /// The SQL expression resulting in a scalar value that can be used as an argument to an operator or function.
    arg_expr: String,

    /// The scalar type returned by the arg_expr SQL expression.
    arg_type: ExpressionReturnType,

    /// The SQL expression resulting in a scalar value representing the true value of the parameter.
    /// This will typically be the same as arg_expr, with the exception that Select/Multiselect/Object columns will have their primary keys
    /// returned by arg_expr and their referenced row OIDs returned by value_expr.
    value_expr: String,

    /// The SQL expression for the label of that scalar value (e.g. primary key of the row referenced by a Select column).
    label_expr: String,

    /// The SQL expression for the parameter returned by the expression, if it returns the value of an unmodified parameter.
    param_expr: String,

    /// True if the expressions are deterministic. False if RANDOM() is invoked.
    deterministic: bool,
}

struct SchemaView {
    param_cte: HashMap<Datasource, ParamCTE>,
    columns: HashMap<i64, SchemaViewColumn>,
    rand_count: usize,
}

impl SchemaView {
    /// Construct an empty schema view object.
    fn new() -> Self {
        Self {
            param_cte: HashMap::new(),
            columns: HashMap::new(),
            rand_count: 0,
        }
    }

    fn compile(self, trans: &Transaction, schema_oid: i64) -> Result<String, Error> {
        // Compile the CTEs and select only from root datasources
        let (with_expr, star_expr, oid_expr, from_expr) = if self.param_cte.len() > 0 {
            let mut with_expr: String = String::from("WITH");
            let mut oid_expr: Vec<String> = Vec::new();
            let mut filter_expr: String = String::from("");
            let mut from_expr: String = String::from("FROM");

            let mut root_datasource: HashSet<Datasource> = HashSet::new();
            let mut all_1_to_1: bool = true;
            for (cte_datasource, cte) in self.param_cte.into_iter() {
                // If the CTE is not being grouped, check if it has a 1-to-* relationship with its root
                if (!cte.is_grouped) {
                    match cte_datasource.seek_basis()? {
                        Datasource::Table { .. } => {},
                        _ => {
                            // If basis datasource is not a root datasource, flag there as being a datasource which is not 1-to-1 with the root
                            all_1_to_1 = false;
                        }
                    }
                }
                
                root_datasource.insert(cte_datasource.seek_root());

                let cte_datasource_alias: String = cte_datasource.get_alias();

                // Compile the CTE
                with_expr = format!(
                    "{with_expr}{} {}",
                    if with_expr == "WITH" { "" } else { "," },
                    cte.compile()?
                );

                // Select OID from the datasource
                oid_expr.push(format!("{cte_datasource_alias}_OID"));
                filter_expr = format!(
                    "{filter_expr}{}{}",
                    if filter_expr == "" { "'" } else { " || '&" },
                    format!("{cte_datasource_alias}=' || CAST({cte_datasource_alias}_OID AS TEXT)")
                );

                // If the datasource is a root, select from it
                if let Datasource::Table { .. } = cte_datasource {
                    from_expr = format!(
                        "{from_expr}{} {}",
                        if from_expr == "FROM" {
                            ""
                        } else {
                            " INNER JOIN"
                        },
                        cte_datasource.get_alias()
                    );
                }
            }
            (
                with_expr,
                {
                    let mut star_columns: Vec<String> = root_datasource
                        .iter()
                        .map(|root_datasource| format!("{}.*", root_datasource.get_alias()))
                        .collect();

                    let mut k: usize = 1;
                    while k <= self.rand_count {
                        star_columns.push(format!("RANDOM() AS RANDOM{k}"));
                        k += 1;
                    }

                    star_columns
                        .into_iter()
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap()
                },
                oid_expr.into_iter().fold(
                    format!(
                        "{} AS QUERY_FILTER{}",
                        if filter_expr == "" {
                            String::from("''")
                        } else {
                            filter_expr
                        },
                        if root_datasource.len() == 1 && all_1_to_1 {
                            let root_datasource: Datasource =
                                root_datasource.into_iter().next().unwrap();
                            format!(
                                ", {} AS TABLE_OID, {}_OID AS OID", 
                                root_datasource.get_schema_oid()?,
                                root_datasource.get_alias()
                            )
                        } else {
                            String::from(", NULL AS TABLE_OID, NULL AS OID")
                        }
                    ),
                    |acc, e| format!("{acc}, {e}"),
                ),
                from_expr,
            )
        } else {
            (
                String::from(""),
                String::from(""),
                String::from("'' AS QUERY_FILTER, NULL AS TABLE_OID, NULL AS OID"),
                String::from(""),
            )
        };

        // Compile the final CTE
        // The purpose of an intermediary CTE before the top-level SELECT is to synchronize random values across value/label/arg/param expressions
        // TODO: Can remove the intermediary CTE if no random values are required?
        let with_expr: String = {
            if with_expr == "" {
                String::from("")
            } else {
                format!(
                    "{with_expr}, {}",
                    format!("FINAL_CTE AS (SELECT {star_expr} {from_expr})")
                )
            }
        };

        // Compile ordering columns
        let mut index_ordering_expr: String = String::from("");
        for row_result in trans.prepare("SELECT COLUMN_OID, SORT_ASCENDING FROM METADATA_SCHEMA_ORDERBY_VIEW WHERE SCHEMA_OID = ?1 ORDER BY ORDERING")?.query_map(params![schema_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, bool>("SORT_ASCENDING")?)))? {
            let (column_oid, sort_ascending) = row_result?;

            if let Some(c) = self.columns.get(&column_oid) {
                if index_ordering_expr == "" {
                    index_ordering_expr = format!("{} {}", c.compile()?, if sort_ascending { "ASC" } else { "DESC" });
                } else {
                    index_ordering_expr = format!("{index_ordering_expr}, {} {}", c.compile()?, if sort_ascending { "ASC" } else { "DESC" });
                }
            }
        }

        // Compile each column
        let column_expr: String = {
            let mut column_expr: Vec<String> = Vec::new();
            for (_, c) in self.columns.into_iter() {
                column_expr.push(c.compile()?);
            }
            column_expr
                .into_iter()
                .fold(oid_expr, |acc, e| format!("{acc}, {e}"))
        };

        // Compile the SELECT
        Ok(format!(
            "
            CREATE VIEW IF NOT EXISTS SCHEMA{schema_oid}_VIEW AS 
            {with_expr}
            SELECT 
                ROW_NUMBER() OVER ({index_ordering_expr}) AS ROW_INDEX, {column_expr}
            {}
            ",
            if with_expr == "" {
                ""
            } else {
                "FROM FINAL_CTE"
            }
        ))
    }

    /// Adds a CTE for params from a datasource.
    fn add_datasource_cte(&mut self, datasource: &Datasource, is_grouped: bool) {
        if !self.param_cte.contains_key(&datasource) {
            // Add the parent datasource
            if let Some(parent_datasource) = datasource.get_parent() {
                self.add_datasource_cte(&parent_datasource, is_grouped.clone());

                // Link the datasource to its parent
                if let Some(parent_datasource_cte) = self.param_cte.get_mut(&parent_datasource) {
                    parent_datasource_cte
                        .child_datasources
                        .insert(datasource.clone());
                }
            }

            // Add a CTE for the datasource
            self.param_cte.insert(
                datasource.clone(),
                ParamCTE {
                    datasource: datasource.clone(),
                    child_datasources: HashSet::new(),
                    columns: HashMap::new(),
                    is_grouped
                },
            );
        } else if let Some(cte) = self.param_cte.get_mut(&datasource) {
            cte.is_grouped = cte.is_grouped && is_grouped;
        }
    }

    ///
    fn add_column(
        &mut self,
        trans: &Transaction,
        root_datasource: &Option<Datasource>,
        datasource_path: String,
        column_metadata: column::FullMetadata,
    ) -> Result<(), Error> {
        match &column_metadata.column_type {
            column_type::ColumnType::Primitive(_)
            | column_type::ColumnType::Object { .. }
            | column_type::ColumnType::Select { .. }
            | column_type::ColumnType::Multiselect { .. } => {
                if let Some(root_datasource) = root_datasource {
                    // Add the primitive column as a param
                    let column_oid: i64 = column_metadata.oid.clone();
                    let column_datasource: Datasource =
                        root_datasource.append_path(datasource_path)?;
                    let (_, access_param) = self.add_param(column_datasource, column_metadata, false)?;

                    // Register the column to the query
                    self.columns.insert(
                        column_oid.clone(),
                        SchemaViewColumn::TableData {
                            label_expr: format!("{}", access_param.label_ord),
                            label_ord: format!("COLUMN{}_LABEL", column_oid),
                            value_expr: format!("{}", access_param.value_ord),
                            value_ord: format!("COLUMN{}_VALUE", column_oid),
                        },
                    );
                } else {
                    return Err(Error::OrphanedDataColumn {
                        column_oid: column_metadata.oid,
                        column_name: column_metadata.name,
                    });
                };
            }
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let parsed_formula: Box<Formula> = Box::new(Formula::parse(formula.clone())?);

                // Compile the formula into SQL
                let scalar_expression: ScalarExpression = self.compile_formula(
                    trans,
                    column_metadata.schema.oid,
                    match root_datasource {
                        Some(root_datasource) => root_datasource.append_path(datasource_path)?,
                        None => Datasource::from_alias_transact(trans, datasource_path)?,
                    },
                    parsed_formula,
                    false
                )?;

                // Turn into a column
                self.columns.insert(
                    column_metadata.oid.clone(),
                    SchemaViewColumn::Formula {
                        label_expr: scalar_expression.label_expr,
                        label_ord: format!("COLUMN{}_LABEL", column_metadata.oid),
                        value_expr: scalar_expression.value_expr,
                        value_ord: format!("COLUMN{}_VALUE", column_metadata.oid),
                        param_expr: scalar_expression.param_expr,
                        param_ord: format!("COLUMN{}_PARAM", column_metadata.oid),
                    },
                );
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                // Iterate through key columns of the report
                for key_column_result in trans.prepare("SELECT c.OID FROM METADATA_SCHEMA_COLUMN_VIEW sc INNER JOIN METADATA_COLUMN c ON c.OID = sc.COLUMN_OID WHERE sc.IS_REQUIRED AND c.IS_PRIMARY_KEY AND sc.SCHEMA_OID = ?1")?.query_map(params![report_oid], |row| row.get::<_, i64>("OID"))? {
                    let key_column_oid: i64 = key_column_result?;
                    let key_column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, key_column_oid)?;

                    // Compile the formula

                }

                let label_expr: String = format!("COALESCE(GROUP_CONCAT(), '')");

                // Turn into a column
                self.columns.insert(
                    column_metadata.oid.clone(),
                    SchemaViewColumn::Subreport { 
                        label_expr, 
                        label_ord: format!("COLUMN{}_LABEL", column_metadata.oid) 
                    }
                );
            }
            _ => {
                // Ignore other virtual column types
            }
        }
        Ok(())
    }

    /// Adds a data cell to a datasource.
    fn add_param(
        &mut self,
        datasource: Datasource,
        column: column::FullMetadata,
        is_grouped: bool
    ) -> Result<(Datasource, ParamCTEColumn), Error> {
        // Ensure the CTE for the datasource exists
        self.add_datasource_cte(&datasource, is_grouped);

        // Add the column to that CTE
        let column_path: String = format!("{}_COLUMN{}", datasource.get_alias(), column.oid);
        let param = if let Some(datasource_cte) = self.param_cte.get_mut(&datasource) {
            if !datasource_cte.columns.contains_key(&column_path) {
                datasource_cte.columns.insert(column_path.clone(), match column.column_type {
                    column_type::ColumnType::Primitive(prim) => {
                        match prim {
                            column_type::Primitive::Checkbox => ParamCTEColumn { 
                                label_expr: format!("CAST(d.COLUMN{} AS TEXT)", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"),  
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Boolean),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::Integer => ParamCTEColumn { 
                                label_expr: format!("CAST(d.COLUMN{} AS TEXT)", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"),  
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::Number => ParamCTEColumn { 
                                label_expr: format!("CAST(d.COLUMN{} AS TEXT)", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"),  
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::Text => ParamCTEColumn { 
                                label_expr: format!("d.COLUMN{}", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"),  
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Text),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::JSON => ParamCTEColumn { 
                                label_expr: format!("d.COLUMN{}", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::JSON),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::Date => ParamCTEColumn { 
                                label_expr: format!("DATE(d.COLUMN{}, 'julianday')", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Date),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::Datetime => ParamCTEColumn { 
                                label_expr: format!("STRFTIME('%FT%TZ', d.COLUMN{}, 'julianday')", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Datetime),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::File | column_type::Primitive::Image => ParamCTEColumn { 
                                label_expr: format!("(SELECT f.LABEL FROM METADATA_FILE_VIEW f WHERE f.OID = d.COLUMN{})", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::File),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        }
                    }
                    column_type::ColumnType::Object { table_oid, .. } => {
                        if column.schema.oid == datasource.get_schema_oid()? {
                            // Is normal
                            ParamCTEColumn {
                                label_expr: format!("(SELECT l.OBJECT_LABEL FROM TABLE{table_oid}_LABEL_VIEW l WHERE l.OID = d.COLUMN{})", column.oid),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("d.COLUMN{}", column.oid),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_record(table_oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        } else {
                            // Is reversed
                            ParamCTEColumn {
                                label_expr: format!("
                                    (
                                        SELECT 
                                            '[' || COALESCE(GROUP_CONCAT(l.JSON_LABEL, ', '), '') || ']'
                                        FROM TABLE{table_oid}_LABEL_VIEW l 
                                        WHERE l.COLUMN{} = d.OID
                                    )
                                    ", 
                                    column.oid
                                ),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("
                                    (
                                        SELECT 
                                            GROUP_CONCAT(CAST(s.OID AS TEXT), ',') 
                                        FROM TABLE{} s
                                        WHERE s.COLUMN{} = d.OID AND NOT s.TRASH
                                    )
                                    ", 
                                    column.schema.oid,
                                    column.oid
                                ),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("
                                    (
                                        SELECT
                                            s.OID
                                        FROM TABLE{} s
                                        WHERE s.COLUMN{} = {}_OID AND NOT s.TRASH
                                    )
                                    ", 
                                    column.schema.oid, 
                                    column.oid,
                                    datasource.get_alias()
                                ), 
                                arg_type: ExpressionReturnType::new_record(column.schema.oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        }
                    }
                    column_type::ColumnType::Select { table_oid, .. } => {
                        if column.schema.oid == datasource.get_schema_oid()? {
                            // Is normal
                            ParamCTEColumn {
                                label_expr: format!("(SELECT l.SELECT_LABEL FROM TABLE{table_oid}_LABEL_VIEW l WHERE l.OID = d.COLUMN{})", column.oid),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("d.COLUMN{}", column.oid),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_record(table_oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        } else {
                            // Is reversed
                            ParamCTEColumn {
                                label_expr: format!("
                                    (
                                        SELECT 
                                            '[' || COALESCE(GROUP_CONCAT(l.JSON_LABEL, ', '), '') || ']'
                                        FROM TABLE{table_oid}_LABEL_VIEW l 
                                        WHERE l.COLUMN{} = d.OID
                                    )
                                    ", 
                                    column.oid
                                ),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("
                                    (
                                        SELECT 
                                            GROUP_CONCAT(CAST(s.OID AS TEXT), ',') 
                                        FROM TABLE{} s
                                        WHERE s.COLUMN{} = d.OID AND NOT s.TRASH
                                    )
                                    ", 
                                    column.schema.oid,
                                    column.oid
                                ),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("
                                    (
                                        SELECT
                                            s.OID
                                        FROM TABLE{} s
                                        WHERE s.COLUMN{} = {}_OID AND NOT s.TRASH
                                    )
                                    ", 
                                    column.schema.oid, 
                                    column.oid,
                                    datasource.get_alias()
                                ), 
                                arg_type: ExpressionReturnType::new_record(column.schema.oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        }
                    }
                    column_type::ColumnType::Multiselect { table_oid, .. } => {
                        if column.schema.oid == datasource.get_schema_oid()? {
                            // Is normal
                            ParamCTEColumn {
                                label_expr: format!("
                                    (
                                        SELECT 
                                            '[' || GROUP_CONCAT(l.JSON_LABEL, ', ') || ']'
                                        FROM MULTISELECT{} m 
                                        INNER JOIN TABLE{table_oid}_LABEL_VIEW l ON l.OID = m.TABLE{table_oid}_OID 
                                        WHERE m.TABLE{}_OID = d.OID
                                    )", 
                                    column.oid,
                                    column.schema.oid
                                ),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("
                                    (
                                        SELECT 
                                            GROUP_CONCAT(CAST(t.OID AS TEXT), ',')
                                        FROM MULTISELECT{} m 
                                        INNER JOIN TABLE{table_oid} t ON t.OID = m.TABLE{table_oid}_OID 
                                        WHERE m.TABLE{}_OID = d.OID AND NOT t.TRASH
                                    )", 
                                    column.oid,
                                    column.schema.oid
                                ),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("
                                    (
                                        SELECT 
                                            TABLE{table_oid}_OID
                                        FROM MULTISELECT{}
                                        WHERE TABLE{}_OID = {}_OID AND TABLE{table_oid}_OID IN (SELECT OID FROM TABLE{table_oid} WHERE NOT TRASH)
                                    )", 
                                    column.oid,
                                    column.schema.oid,
                                    datasource.get_alias()
                                ),
                                arg_type: ExpressionReturnType::new_record(table_oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        } else {
                            // Is reversed
                            ParamCTEColumn {
                                label_expr: format!("
                                    (
                                        SELECT 
                                            '[' || GROUP_CONCAT(l.JSON_LABEL, ', ') || ']'
                                        FROM MULTISELECT{} m 
                                        INNER JOIN TABLE{}_LABEL_VIEW l ON l.OID = m.TABLE{}_OID 
                                        WHERE m.TABLE{table_oid}_OID = d.OID
                                    )", 
                                    column.oid,
                                    column.schema.oid,
                                    column.schema.oid
                                ),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("
                                    (
                                        SELECT 
                                            GROUP_CONCAT(CAST(t.OID AS TEXT), ',')
                                        FROM MULTISELECT{} m 
                                        INNER JOIN TABLE{} t ON t.OID = m.TABLE{}_OID 
                                        WHERE m.TABLE{table_oid}_OID = d.OID AND NOT t.TRASH
                                    )", 
                                    column.oid,
                                    column.schema.oid,
                                    column.schema.oid 
                                ),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("
                                    (
                                        SELECT 
                                            t.OID
                                        FROM MULTISELECT{} m 
                                        INNER JOIN TABLE{} t ON t.OID = m.TABLE{}_OID 
                                        WHERE m.TABLE{table_oid}_OID = d.OID AND NOT t.TRASH
                                    )", 
                                    column.oid,
                                    column.schema.oid,
                                    column.schema.oid 
                                ),
                                arg_type: ExpressionReturnType::new_record(column.schema.oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        }
                    }
                    _ => {
                        // Column cannot be added as a parameter
                        return Err(Error::InvalidParameter { 
                            column_oid: column.oid, 
                            column_name: column.name, 
                            column_type: column.column_type.to_str() 
                        });
                    }
                });
            }

            datasource_cte.columns[&column_path].clone()
        } else {
            return Err(Error::InvalidDatasource {
                datasource_alias: datasource.get_alias(),
            });
        };
        Ok((datasource.seek_root(), param))
    }

    /// Compile the formula into SQL.
    fn compile_formula(
        &mut self,
        trans: &Transaction,
        root_oid: i64,
        root_datasource: Datasource,
        formula: Box<Formula>,
        is_grouped: bool
    ) -> Result<ScalarExpression, Error> {
        Ok(match *formula {
            Formula::Null => ScalarExpression {
                arg_expr: String::from("NULL"),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Null),
                value_expr: String::from("NULL"),
                label_expr: String::from("NULL"),
                param_expr: String::from("NULL"),
                deterministic: true,
            },
            Formula::LiteralBool(b) => {
                let (value_expr, label_expr) = if b {
                    (String::from("TRUE"), String::from("'True'"))
                } else {
                    (String::from("FALSE"), String::from("'False'"))
                };
                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Boolean),
                    value_expr,
                    label_expr,
                    param_expr: String::from("'boolean'"),
                    deterministic: true,
                }
            }
            Formula::LiteralInt(num) => ScalarExpression {
                arg_expr: format!("{num}"),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                value_expr: format!("{num}"),
                label_expr: format!("'{num}'"),
                param_expr: String::from("'integer'"),
                deterministic: true,
            },
            Formula::LiteralFloat(num) => ScalarExpression {
                arg_expr: format!("{num}"),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number),
                value_expr: format!("{num}"),
                label_expr: format!("'{num}'"),
                param_expr: String::from("'number'"),
                deterministic: true,
            },
            Formula::LiteralString(str) => {
                let safe_str: String = format!("'{}'", str.replace("'", "''"));
                ScalarExpression {
                    arg_expr: safe_str.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Text),
                    value_expr: safe_str.clone(),
                    label_expr: safe_str.clone(),
                    param_expr: String::from("'textplain'"),
                    deterministic: true,
                }
            }
            Formula::RandomInt => {
                self.rand_count += 1;
                ScalarExpression {
                    arg_expr: format!("RANDOM{}", self.rand_count),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                    value_expr: format!("RANDOM{}", self.rand_count),
                    label_expr: format!("CAST(RANDOM{} AS TEXT)", self.rand_count),
                    param_expr: String::from("'integer'"),
                    deterministic: false,
                }
            }
            Formula::Param {
                datasource_alias,
                column_oid,
            } => {
                let column_datasource: Datasource =
                    Datasource::from_alias(datasource_alias.clone())?
                        .substitute_root(root_oid.clone(), root_datasource.clone());
                let column_metadata =
                    column::FullMetadata::get_transact(trans, column_oid.clone())?;
                match &column_metadata.column_type {
                    column_type::ColumnType::Primitive(_)
                    | column_type::ColumnType::Object { .. }
                    | column_type::ColumnType::Select { .. }
                    | column_type::ColumnType::Multiselect { .. } => {
                        let param_name: String = match &column_metadata.column_type {
                            column_type::ColumnType::Primitive(prim) => String::from(match prim {
                                column_type::Primitive::Checkbox => "boolean",
                                column_type::Primitive::Integer => "integer",
                                column_type::Primitive::Number => "number",
                                column_type::Primitive::Text => "text/plain",
                                column_type::Primitive::JSON => "text/JSON",
                                column_type::Primitive::File => "file/any",
                                column_type::Primitive::Image => "file/image",
                                column_type::Primitive::Date => "dateonly",
                                column_type::Primitive::Datetime => "datetime",
                            }),
                            column_type::ColumnType::Object { table_oid, .. } => {
                                format!("object{table_oid}")
                            }
                            column_type::ColumnType::Select { table_oid, .. } => {
                                format!("select{table_oid}")
                            }
                            column_type::ColumnType::Multiselect { table_oid, .. } => {
                                format!("multiselect{table_oid}")
                            }
                            _ => String::from("N/A"), // This case shouldn't happen
                        };

                        let (_, param) = self.add_param(column_datasource, column_metadata, is_grouped)?;

                        // Parameter expressions return a string in the form "{TABLE_OID}:{COLUMN_OID}:{ROW_OID}"
                        let param_expr: String = if let Some(param_cell) = param.cell {
                            format!(
                                "('{param_name}:{}:{}:' || CAST({} AS TEXT))",
                                param_cell.table_oid, param_cell.column_oid, param_cell.row_ord
                            )
                        } else {
                            String::from("NULL") // This case shouldn't happen?
                        };

                        // Bubble up the expression to get the parameter
                        ScalarExpression {
                            arg_expr: param.arg_expr,
                            arg_type: param.arg_type,
                            value_expr: param.value_expr,
                            label_expr: param.label_expr,
                            param_expr,
                            deterministic: true,
                        }
                    }
                    column_type::ColumnType::Formula { formula, .. } => {
                        // Parse the formula
                        let parsed_formula: Box<Formula> =
                            Box::new(Formula::parse(formula.clone())?);

                        // Compile the formula into a scalar expression
                        let formula_root_oid: i64 =
                            match Datasource::get_default_datasource_transact(
                                trans,
                                column_datasource.get_schema_oid()?,
                            )? {
                                Datasource::Table { oid, .. } => oid,
                                _ => {
                                    // This case should not ever occur, but just in case...
                                    return Err(Error::AdhocError("get_default_datasource_transact() function did not return a Datasource::Table, which is not allowed."));
                                }
                            };
                        self.compile_formula(
                            trans,
                            formula_root_oid,
                            column_datasource,
                            parsed_formula,
                            is_grouped
                        )?
                    }
                    _ => {
                        // Column type is not allowed to be used as a parameter in a formula
                        return Err(Error::InvalidParameter {
                            column_oid,
                            column_name: column_metadata.name,
                            column_type: column_metadata.column_type.to_str(),
                        });
                    }
                }
            }
            Formula::Coalesce(items) => {
                let mut items_compiled: Vec<ScalarExpression> = Vec::new();
                for item in items {
                    let item_compiled: ScalarExpression = self.compile_formula(
                        trans,
                        root_oid.clone(),
                        root_datasource.clone(),
                        Box::new(item),
                        is_grouped.clone()
                    )?;
                    items_compiled.push(item_compiled);
                }

                let deterministic: bool = items_compiled
                    .iter()
                    .all(|item_compiled| item_compiled.deterministic);
                let arg_type: ExpressionReturnType = items_compiled.iter().fold(
                    ExpressionReturnType::new_primitive(PrimitiveScalarType::Null),
                    |acc, item_compiled| acc.generalize(&item_compiled.arg_type),
                );
                let (label_expr, param_expr) = if items_compiled.len() > 1 {
                    (
                        format!(
                            "{} ELSE {} END",
                            items_compiled.iter().take(items_compiled.len() - 1).fold(
                                String::from("CASE"),
                                |acc, item_compiled| format!(
                                    "{acc} WHEN {} IS NOT NULL THEN {}",
                                    item_compiled.value_expr, item_compiled.label_expr
                                )
                            ),
                            items_compiled[items_compiled.len() - 1].label_expr
                        ),
                        format!(
                            "{} ELSE {} END",
                            items_compiled.iter().take(items_compiled.len() - 1).fold(
                                String::from("CASE"),
                                |acc, item_compiled| format!(
                                    "{acc} WHEN {} IS NOT NULL THEN {}",
                                    item_compiled.value_expr, item_compiled.param_expr
                                )
                            ),
                            items_compiled[items_compiled.len() - 1].param_expr
                        ),
                    )
                } else if items_compiled.len() == 1 {
                    (
                        items_compiled[0].label_expr.clone(),
                        items_compiled[0].param_expr.clone(),
                    )
                } else {
                    (String::from("NULL"), String::from("NULL"))
                };
                let (value_expr, arg_expr) = match items_compiled
                    .into_iter()
                    .map(|item_compiled| (item_compiled.value_expr, item_compiled.arg_expr))
                    .reduce(|(acc_value, acc_arg), (e_value, e_arg)| {
                        (
                            format!("{acc_value}, {e_value}"),
                            format!("{acc_arg}, {e_arg}"),
                        )
                    }) {
                    Some((acc_value, acc_arg)) => (
                        format!("COALESCE({acc_value})"),
                        format!("COALESCE({acc_arg})"),
                    ),
                    None => (String::from("NULL"), String::from("NULL")),
                };

                ScalarExpression {
                    arg_expr,
                    arg_type,
                    value_expr,
                    label_expr,
                    param_expr,
                    deterministic,
                }
            }
            Formula::Abs(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression =
                    self.compile_formula(trans, root_oid, root_datasource, inner, is_grouped)?;
                if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number)
                    .accepts_arg(&inner_compiled.arg_type)
                {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "abs",
                        inner_name,
                        expected_type: ExpressionReturnType::new_primitive(
                            PrimitiveScalarType::Number,
                        )
                        .to_string(trans),
                        received_type: inner_compiled.arg_type.to_string(trans),
                    });
                }

                let value_expr: String = format!("ABS({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: inner_compiled.arg_type,
                    label_expr,
                    value_expr,
                    param_expr: inner_compiled.param_expr,
                    deterministic: inner_compiled.deterministic,
                }
            }
            Formula::Sign(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression =
                    self.compile_formula(trans, root_oid, root_datasource, inner, is_grouped)?;
                if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number)
                    .accepts_arg(&inner_compiled.arg_type)
                {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "sign",
                        inner_name,
                        expected_type: ExpressionReturnType::new_primitive(
                            PrimitiveScalarType::Number,
                        )
                        .to_string(trans),
                        received_type: inner_compiled.arg_type.to_string(trans),
                    });
                }

                let value_expr: String = format!("SIGN({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                    label_expr,
                    value_expr,
                    param_expr: String::from("'integer'"),
                    deterministic: inner_compiled.deterministic,
                }
            }
            Formula::Floor(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression =
                    self.compile_formula(trans, root_oid, root_datasource, inner, is_grouped)?;
                if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number)
                    .accepts_arg(&inner_compiled.arg_type)
                {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "floor",
                        inner_name,
                        expected_type: ExpressionReturnType::new_primitive(
                            PrimitiveScalarType::Number,
                        )
                        .to_string(trans),
                        received_type: inner_compiled.arg_type.to_string(trans),
                    });
                }

                let value_expr: String = format!("FLOOR({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                    label_expr,
                    value_expr,
                    param_expr: String::from("'integer'"),
                    deterministic: inner_compiled.deterministic,
                }
            }
            Formula::Ceiling(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression =
                    self.compile_formula(trans, root_oid, root_datasource, inner, is_grouped)?;
                if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number)
                    .accepts_arg(&inner_compiled.arg_type)
                {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "ceil",
                        inner_name,
                        expected_type: ExpressionReturnType::new_primitive(
                            PrimitiveScalarType::Number,
                        )
                        .to_string(trans),
                        received_type: inner_compiled.arg_type.to_string(trans),
                    });
                }

                let value_expr: String = format!("CEILING({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                    label_expr,
                    value_expr,
                    param_expr: String::from("'integer'"),
                    deterministic: inner_compiled.deterministic,
                }
            }
            Formula::Round(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression =
                    self.compile_formula(trans, root_oid, root_datasource, inner, is_grouped)?;
                if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number)
                    .accepts_arg(&inner_compiled.arg_type)
                {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "round",
                        inner_name,
                        expected_type: ExpressionReturnType::new_primitive(
                            PrimitiveScalarType::Number,
                        )
                        .to_string(trans),
                        received_type: inner_compiled.arg_type.to_string(trans),
                    });
                }

                let value_expr: String = format!("ROUND({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                    label_expr,
                    value_expr,
                    param_expr: String::from("'integer'"),
                    deterministic: inner_compiled.deterministic,
                }
            }
            _ => {
                todo!("Function {} is not implemented yet!", formula.to_string());
            }
        })
    }

    /// Compile a subreport label.
    fn compile_subreport_label(
        &mut self,
        trans: &Transaction,
        root_oid: i64,
        root_datasource: Datasource,
        formula: Box<Formula>,
        is_grouped: bool
    ) {
        
    }
}

/// Create a view for the table.
fn create_schema_view(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    println!("Creating schema view SCHEMA{schema_oid}_VIEW...");

    // Get the root table datasource for the view
    let mut view: SchemaView = SchemaView::new();
    let root_datasource: Option<Datasource> = if let Some(root_datasource_oid) = trans
        .query_one(
            "SELECT OID FROM METADATA_DATASOURCE WHERE TABLE_OID = ?1 LIMIT 1",
            params![schema_oid],
            |row| row.get("OID"),
        )
        .optional()?
    {
        let root_datasource: Datasource = Datasource::get_transact(trans, root_datasource_oid)?;
        view.add_datasource_cte(&root_datasource, false);
        Some(root_datasource)
    } else {
        None
    };

    // Add each column that belongs to the schema
    for row_result in trans.prepare("SELECT DATASOURCE_PATH, COLUMN_OID FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_REQUIRED")?.query_map(params![schema_oid], |row| Ok((row.get::<_, String>("DATASOURCE_PATH")?, row.get::<_, i64>("COLUMN_OID")?)))? {
        let (datasource_path, column_oid) = row_result?;
        let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid.clone())?;
        view.add_column(trans, &root_datasource, datasource_path, column_metadata)?;
    }

    let create_sql: String = view.compile(trans, schema_oid)?;
    println!("{create_sql}");
    trans.execute(&create_sql, [])?;
    Ok(())
}

/// Create the views associated with a schema.
pub fn regenerate_schema_views(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    // Drop existing views that are dependent on these
    let mut view_creation_ordering: Vec<ViewsToCreate> = Vec::new();
    drop_all_views(trans, schema_oid, &mut view_creation_ordering)?;

    // Create all of the polymorphism views
    for v in view_creation_ordering.iter() {
        let table_name: String = format!("TABLE{}", v.schema_oid);
        if v.create_polymorphism_view && trans.table_exists(Some("main"), &table_name)? {
            create_table_polymorphism_view(trans, v.schema_oid.clone())?;
        }
    }

    // Create all of the label views
    for v in view_creation_ordering.iter() {
        let table_name: String = format!("TABLE{}", v.schema_oid);
        if v.create_label_view && trans.table_exists(Some("main"), &table_name)? {
            create_table_label_view(trans, v.schema_oid.clone())?;
        }
    }

    // Create all of the schema views
    for v in view_creation_ordering.iter() {
        create_schema_view(trans, v.schema_oid)?;
    }
    Ok(())
}
