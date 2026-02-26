use rusqlite::{Connection, Transaction, params};
use crate::data::{column, column_type, datasource, table};
use crate::util::db;
use crate::util::error::Error;
use std::collections::HashMap;


pub struct Parameter {
    datasource: datasource::Datasource,
    column: column::Metadata
}



#[derive(Clone)]
enum Relationship {
    One,
    Many {
        intermediate_param_oid: Vec<i64>,
        final_param_oid: i64
    }
}

struct ReportPrimaryDatasource {
    oid: i64,
    datasource_oid: i64 
}

enum PrimaryDatasource {
    Table(table::Metadata),
    ReportDatasources(Vec<ReportPrimaryDatasource>)
}

impl PrimaryDatasource {
    /// Checks if the given datasource is one of the primary datasources, and returns its alias if so.
    fn get_alias(&self, datasource_oid: i64) -> Option<String> {
        match self {
            Self::Table(table_metadata) => {
                if table_metadata.datasource_oid == datasource_oid {
                    return Some(format!("t{}", table_metadata.schema.oid));
                } else {
                    return None;
                }
            }
            Self::ReportDatasources(report_datasources) => {
                for report_datasource in report_datasources.iter() {
                    if report_datasource.oid == datasource_oid {
                        return Some(format!("d{datasource_oid}"));
                    }
                }
                return None;
            }
        }
    }
}



#[derive(Clone)]
struct Join {
    /// The table that was joined to.
    table_oid: i64,

    /// The alias for the joined table.
    alias: String,

    /// The relationship of the join to the base table.
    relation: Relationship 
}

struct Param {
    /// The count of rows for this param.
    count: Relationship,

    /// The OID of the table where this param comes from.
    table_oid: i64,

    /// The OID of the column where this param comes from.
    column_oid: i64,

    /// The ordinal that can be pulled to determine the OID of the row where this param comes from.
    row_ord: String,

    /// The type of the column.
    column_type: column_type::ColumnType,

    /// The ordinal that can be pulled to get the true value of the column.
    true_ord: Option<String>
}

struct SelectStatement {
    /// The primary datasources of the query. Maps from the alias to the datasource OID.
    primary_datasource: PrimaryDatasource,

    /// The datasources joined through a parameter.
    /// Maps from the datasource OID to the join definition.
    secondary_datasources: HashMap<i64, Join>,

    /// The join definitions.
    cmd_tbls: String,

    /// The parameters selected by the query.
    params: HashMap<i64, Param>,

    /// The column definitions.
    cmd_cols: String
}

impl SelectStatement {
    /// Creates a new select statement whose primary datasource is a table.
    fn new_for_table(table_metadata: table::Metadata) -> Self {
        let table_alias: String = format!("t{}", table_metadata.datasource_oid);
        Self {
            cmd_cols: format!("{table_alias}.OID AS {table_alias}_OID"),
            cmd_tbls: format!("FROM TABLE{} {table_alias}", table_metadata.schema.oid),
            primary_datasource: PrimaryDatasource::Table(table_metadata),
            secondary_datasources: HashMap::new(),
            params: HashMap::new(),
        }
    }

    /// Add a raw column definition to the query.
    fn insert_col(&mut self, col_definition: String) {
        self.cmd_cols = format!("{}, {col_definition}", self.cmd_cols);
    }

    /// Add a raw join definition to the query.
    fn insert_tbl(&mut self, tbl_definition: String) {
        self.cmd_tbls = format!("{} {tbl_definition}", self.cmd_tbls);
    }
    

    /// Add a parameter selected by the query.
    fn insert_param(&mut self, param_oid: i64) -> Result<(), Error> {
        // First, check to make sure the parameter hasn't already been added
        if self.params.contains_key(&param_oid) {
            return Ok(());
        }

        // Start a transaction to retrieve details of the parameter
        let conn = db::open()?;

        // Retrieve all parameters it is immediately dependent on
        let (
            column_oid,
            datasource_oid
        ) = conn.query_one(
            "
            SELECT
                COLUMN_OID,
                DATASOURCE_OID
            FROM METADATA_PARAMETER 
            WHERE OID = ?1
            ",
            params![param_oid],
            |row| {
                Ok((
                    row.get::<_, i64>("COLUMN_OID")?,
                    row.get::<_, i64>("DATASOURCE_OID")?
                ))
            }
        )?;
        let column_metadata: column::Metadata = column::Metadata::get(column_oid)?;
        
        // Make sure any parameter this one is dependent on is added to the query
        let relationship: Relationship;
        let source_alias: String = match self.primary_datasource.get_alias(datasource_oid) {
            Some(a) => {
                relationship = Relationship::One;
                a
            },
            None => {
                let dependency_join = self.insert_datasource(&conn, datasource_oid)?;
                relationship = dependency_join.relation;
                dependency_join.alias
            }
        };

        // Construct the query used to retrieve the parameter data
        let true_ord: Option<String> = match &column_metadata.column_type {
            data_type::MetadataColumnType::Primitive(prim) => {
                match prim {
                    data_type::Primitive::Any
                    | data_type::Primitive::Boolean
                    | data_type::Primitive::Integer
                    | data_type::Primitive::Number
                    | data_type::Primitive::Text
                    | data_type::Primitive::JSON => {
                        self.insert_col(
                            format!("CAST({source_alias}.COLUMN{column_oid} AS TEXT) AS PARAM{param_oid}")
                        );
                    }
                    data_type::Primitive::Date => {
                        self.insert_col(
                            format!("
                            DATE({source_alias}.COLUMN{column_oid}, 'julianday') AS PARAM{param_oid}")
                        );
                    }
                    data_type::Primitive::Timestamp => {
                        self.insert_col(
                            format!("STRFTIME('%FT%TZ', {source_alias}.COLUMN{column_oid}, 'julianday') AS PARAM{param_oid}")
                        );
                    }
                    data_type::Primitive::File => {
                        self.insert_col(
                            format!("
                            CASE 
                            WHEN {source_alias}.COLUMN{column_oid} IS NULL THEN NULL 
                            ELSE 
                                CASE 
                                    WHEN LENGTH({source_alias}.COLUMN{column_oid}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.000000001)
                                    WHEN LENGTH({source_alias}.COLUMN{column_oid}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.000001)
                                    ELSE FORMAT('%.1f KB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.001)
                                END
                            END AS PARAM{param_oid}
                            ")
                        );
                    }
                    data_type::Primitive::Image => {
                        self.insert_col(
                            format!("CASE WHEN {source_alias}.COLUMN{column_oid} IS NULL THEN NULL ELSE 'Thumbnail' END AS PARAM{param_oid}")
                        );
                    }
                }

                Some(format!("PARAM{param_oid}"))
            },
            data_type::MetadataColumnType::SingleSelectDropdown(_) => {
                self.insert_col(format!("p{param_oid}.VALUE AS PARAM{param_oid}"));
                self.insert_col(format!("CAST(p{param_oid}.OID AS TEXT) AS _PARAM{param_oid}"));
                self.insert_join(format!("LEFT JOIN TABLE{column_type_oid} p{param_oid} ON p{param_oid}.OID = {source_alias}.COLUMN{column_oid}"));

                Some(format!("_PARAM{param_oid}"))
            },
            data_type::MetadataColumnType::MultiSelectDropdown(_) => {
                self.insert_col(
                    format!("(
                    SELECT 
                        '[' || GROUP_CONCAT(b.VALUE) || ']' 
                    FROM TABLE{column_type_oid}_MULTISELECT a 
                    INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID 
                    WHERE a.ROW_OID = {source_alias}.OID 
                    GROUP BY a.ROW_OID) AS PARAM{param_oid}")
                );
                self.insert_col(
                    format!("(
                    SELECT 
                        GROUP_CONCAT(CAST(b.OID AS TEXT))
                    FROM TABLE{column_type_oid}_MULTISELECT a 
                    INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID 
                    WHERE a.ROW_OID = {source_alias}.OID 
                    GROUP BY a.ROW_OID) AS _PARAM{param_oid}")
                );
                
                Some(format!("_PARAM{param_oid}"))
            },
            data_type::MetadataColumnType::Reference(_)
            | data_type::MetadataColumnType::ChildObject(_) => {
                self.insert_col(format!("p{param_oid}.DISPLAY_VALUE AS PARAM{param_oid}"));
                self.insert_col(format!("CAST(p{param_oid}.OID AS TEXT) AS _PARAM{param_oid}"));
                self.insert_join(format!("LEFT JOIN TABLE{column_type_oid}_SURROGATE p{param_oid} ON p{param_oid}.OID = {source_alias}.COLUMN{column_oid}"));

                Some(format!("_PARAM{param_oid}"))
            },
            data_type::MetadataColumnType::ChildTable(_) => {
                self.insert_col(
                    format!("(
                    SELECT 
                        '[' || GROUP_CONCAT(a.DISPLAY_VALUE) || ']' 
                    FROM TABLE{column_type_oid}_SURROGATE a 
                    INNER JOIN TABLE{column_type_oid} b ON b.OID = a.OID 
                    WHERE b.PARENT_OID = {source_alias}.OID 
                    GROUP BY b.PARENT_OID
                    ) AS PARAM{param_oid}")
                );

                None
            }
        }
            
        self.params.insert(param_oid, Param {
            table_oid,
            column_oid,
            row_ord: format!("{source_alias}_OID"),
            column_type,
            true_ord,
            count: relationship
        });

        return Ok(());
    }

    /// Add a join parameter.
    fn insert_datasource(&mut self, conn: &Connection, datasource_oid: i64) -> Result<Join, Error> {
        // First, check to make sure the datasource hasn't already been added
        if self.secondary_datasources.contains_key(&datasource_oid) {
            return Ok(self.secondary_datasources[&datasource_oid].clone());
        }

        // Then, make sure to add any parameter it is dependent on
        let (table_oid, dependency_param_oid, join_statement, is_many) = conn.query_one(
            "
            -- Links through a Object column in the base table
            SELECT 
                t.TABLE_OID,
                p.DATASOURCE_OID AS DEPENDENT_DATASOURCE_OID,
                'LEFT JOIN TABLE' || FORMAT('%d', t.TABLE_OID) || ' d' || FORMAT('%d', p.OID) || 
                    ' ON d' || FORMAT('%d', p.DATASOURCE_OID) || '.COLUMN' || FORMAT('%d', c.OID) || 
                    ' = d' || FORMAT('%d', p.OID) || '.OID'
                AS JOIN_STATEMENT,
                FALSE AS IS_MANY
            FROM METADATA_PARAMETER p
            INNER JOIN METADATA_COLUMN c ON c.OID = p.COLUMN_OID
            INNER JOIN METADATA_COLUMN_TYPE__OBJECT t ON t.OID = c.TYPE_OID
            WHERE p.OID = ?1

            UNION

            -- Links through a Select column in the base table
            SELECT 
                t.TABLE_OID,
                p.DATASOURCE_OID AS DEPENDENT_DATASOURCE_OID,
                'LEFT JOIN TABLE' || FORMAT('%d', t.TABLE_OID) || ' d' || FORMAT('%d', p.OID) || 
                    ' ON d' || FORMAT('%d', p.DATASOURCE_OID) || '.COLUMN' || FORMAT('%d', c.OID) || 
                    ' = d' || FORMAT('%d', p.OID) || '.OID'
                AS JOIN_STATEMENT,
                FALSE AS IS_MANY
            FROM METADATA_PARAMETER p
            INNER JOIN METADATA_COLUMN c ON c.OID = p.COLUMN_OID
            INNER JOIN METADATA_COLUMN_TYPE__SELECT t ON t.OID = c.TYPE_OID
            WHERE p.OID = ?1

            UNION

            -- Links through a Multiselect column in the base table
            SELECT 
                t.TABLE_OID,
                p.DATASOURCE_OID AS DEPENDENT_DATASOURCE_OID,
                'LEFT JOIN MULTISELECT' || FORMAT('%d', c.OID) || ' d' || FORMAT('%d', p.OID) || '_d' || FORMAT('%d', p.DATASOURCE_OID) || 
                    ' ON d' || FORMAT('%d', p.OID) || '_d' || FORMAT('%d', p.DATASOURCE_OID) || '.TABLE' || FORMAT('%d', t.TABLE_OID) ||
                    '_OID = d' || FORMAT('%d', p.DATASOURCE_OID) || '.OID ' ||
                    'LEFT JOIN TABLE' || FORMAT('%d', t.TABLE_OID) || ' d' || FORMAT('%d', p.OID) || 
                    ' ON d' || FORMAT('%d', p.DATASOURCE_OID) || '.COLUMN' || FORMAT('%d', c.OID) || 
                    ' = d' || FORMAT('%d', p.OID) || '.OID'
                AS JOIN_STATEMENT,
                TRUE AS IS_MANY
            FROM METADATA_PARAMETER p
            INNER JOIN METADATA_COLUMN c ON c.OID = p.COLUMN_OID
            INNER JOIN METADATA_COLUMN_TYPE__MULTISELECT t ON t.OID = c.TYPE_OID
            WHERE p.OID = ?1

            UNION

            WITH RECURSIVE JOIN_STATEMENTS (RPT_PARAMETER_OID, TABLE_OID, DEPENDENCY_RPT_PARAMETER_OID, JOIN_STATEMENT, IS_MANY) AS (
                
                SELECT
                    c.RPT_PARAMETER_OID,
                    c.TYPE_OID AS TABLE_OID,
                    NULL AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', c.TYPE_OID) || ' p' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' ON ' || 
                        CASE 
                            WHEN typ.MODE = 5 THEN 'p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.PARENT_OID = t.OID'
                            ELSE 't.COLUMN' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' = p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.OID'
                        END
                    AS JOIN_STATEMENT,
                    CASE WHEN typ.MODE = 5 THEN 1 ELSE 0 END AS IS_MANY
                FROM METADATA_TABLE_COLUMN c
                INNER JOIN METADATA_TYPE typ ON typ.OID = c.TYPE_OID
                WHERE c.TABLE_OID = :base_table_oid AND typ.MODE IN (3,4,5)

                UNION

                -- Links through a reference to the base table
                SELECT
                    c.RPT_PARAMETER_OID,
                    c.TABLE_OID AS TABLE_OID,
                    NULL AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', c.TABLE_OID) || ' p' || FORMAT('%d', c.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.COLUMN' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' = t.OID'
                    AS JOIN_STATEMENT,
                    1 AS IS_MANY
                FROM METADATA_TABLE_COLUMN c
                WHERE c.TYPE_OID = :base_table_oid

                UNION 

                -- Links through inheritance from base table
                SELECT
                    inh.RPT_PARAMETER_OID,
                    inh.INHERITOR_TABLE_OID AS TABLE_OID,
                    NULL AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', inh.INHERITOR_TABLE_OID) || ' p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || '.MASTER' || FORMAT('%d', inh.MASTER_TABLE_OID) || '_OID = t.OID'
                    AS JOIN_STATEMENT,
                    0 AS IS_MANY
                FROM METADATA_TABLE_INHERITANCE inh
                WHERE inh.MASTER_TABLE_OID = :base_table_oid

                UNION 

                -- Links through inheritance by base table
                SELECT
                    inh.RPT_PARAMETER_OID,
                    inh.MASTER_TABLE_OID AS TABLE_OID,
                    NULL AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', inh.MASTER_TABLE_OID) || ' p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || '.OID = t.MASTER' || FORMAT('%d', inh.MASTER_TABLE_OID) || '_OID = t.OID'
                    AS JOIN_STATEMENT,
                    0 AS IS_MANY
                FROM METADATA_TABLE_INHERITANCE inh
                WHERE inh.INHERITOR_TABLE_OID = :base_table_oid

                UNION

                -- Chained link that terminates in a column
                SELECT
                    ch.RPT_PARAMETER_OID,
                    c.TYPE_OID AS TABLE_OID,
                    ch.REF_RPT_PARAMETER_OID AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', c.TYPE_OID) || ' p' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' ON ' || 
                        CASE 
                            WHEN typ.MODE = 5 THEN 'p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.PARENT_OID = t.OID'
                            ELSE 't.COLUMN' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' = p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.OID'
                        END
                    AS JOIN_STATEMENT,
                    CASE WHEN typ.MODE = 5 THEN 1 ELSE 0 END AS IS_MANY
                FROM METADATA_RPT_PARAMETER__CHAIN ch
                INNER JOIN JOIN_STATEMENTS j ON j.RPT_PARAMETER_OID = ch.REF_RPT_PARAMETER_OID
                INNER JOIN METADATA_TABLE_COLUMN c ON c.RPT_PARAMETER_OID = ch.DEF_RPT_PARAMETER_OID AND c.TABLE_OID = j.TABLE_OID
                INNER JOIN METADATA_TYPE typ ON typ.OID = c.TYPE_OID AND typ.MODE IN (3,4,5)

                UNION

                -- Chained link that terminates in the table being referenced by another
                SELECT
                    ch.RPT_PARAMETER_OID,
                    c.TABLE_OID AS TABLE_OID,
                    ch.REF_RPT_PARAMETER_OID AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', c.TABLE_OID) || ' p' || FORMAT('%d', c.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.COLUMN' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' = t.OID'
                    AS JOIN_STATEMENT,
                    1 AS IS_MANY
                FROM METADATA_RPT_PARAMETER__CHAIN ch
                INNER JOIN JOIN_STATEMENTS j ON j.RPT_PARAMETER_OID = ch.REF_RPT_PARAMETER_OID
                INNER JOIN METADATA_TABLE_COLUMN c ON c.RPT_PARAMETER_OID = ch.DEF_RPT_PARAMETER_OID AND c.TYPE_OID = j.TABLE_OID

                UNION 

                -- Chained link that terminates in inheritance from the table
                SELECT
                    ch.RPT_PARAMETER_OID,
                    inh.INHERITOR_TABLE_OID AS TABLE_OID,
                    ch.REF_RPT_PARAMETER_OID AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', inh.INHERITOR_TABLE_OID) || ' p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || '.MASTER' || FORMAT('%d', inh.MASTER_TABLE_OID) || '_OID = t.OID'
                    AS JOIN_STATEMENT,
                    0 AS IS_MANY
                FROM METADATA_RPT_PARAMETER__CHAIN ch
                INNER JOIN JOIN_STATEMENTS j ON j.RPT_PARAMETER_OID = ch.REF_RPT_PARAMETER_OID
                INNER JOIN METADATA_TABLE_INHERITANCE inh ON inh.MASTER_TABLE_OID = j.TABLE_OID

                UNION 

                -- Chained link that terminates in inheritance by the table
                SELECT
                    ch.RPT_PARAMETER_OID,
                    inh.MASTER_TABLE_OID AS TABLE_OID,
                    ch.REF_RPT_PARAMETER_OID AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', inh.MASTER_TABLE_OID) || ' p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || '.OID = t.MASTER' || FORMAT('%d', inh.MASTER_TABLE_OID) || '_OID = t.OID'
                    AS JOIN_STATEMENT,
                    0 AS IS_MANY
                FROM METADATA_RPT_PARAMETER__CHAIN ch
                INNER JOIN JOIN_STATEMENTS j ON j.RPT_PARAMETER_OID = ch.REF_RPT_PARAMETER_OID
                INNER JOIN METADATA_TABLE_INHERITANCE inh ON inh.INHERITOR_TABLE_OID = j.TABLE_OID
            )
            SELECT
                TABLE_OID,
                DEPENDENCY_RPT_PARAMETER_OID,
                JOIN_STATEMENT,
                IS_MANY
            FROM JOIN_STATEMENTS
            WHERE RPT_PARAMETER_OID = :rpt_param_oid
            ",
            named_params! { ":base_table_oid": self.base_table_oid, ":rpt_param_oid": param_oid },
            |row| {
                Ok((
                    row.get::<_, i64>("TABLE_OID")?,
                    row.get::<_, Option<i64>>("DEPENDENCY_RPT_PARAMETER_OID")?,
                    row.get::<_, String>("JOIN_STATEMENT")?,
                    row.get::<_, bool>("IS_MANY")?
                ))
            }
        )?;
        
        // Make sure any parameter this one is dependent on is added to the query
        let dependent_relationship: Relationship = if let Some(o) = dependency_param_oid {
            self.insert_datasource(trans, o)?
        } else {
            Relationship::One
        };
        let this_relationship: Relationship = if is_many {
            match dependent_relationship {
                Relationship::One => {
                    Relationship::Many {
                        intermediate_param_oid: Vec::new(),
                        final_param_oid: param_oid.clone()
                    }
                }
                Relationship::Many { intermediate_param_oid, final_param_oid } => {
                    let mut intermediate_param_oid = intermediate_param_oid.clone();
                    intermediate_param_oid.push(final_param_oid);
                    Relationship::Many { 
                        intermediate_param_oid, 
                        final_param_oid: param_oid.clone()
                    }
                }
            }
        } else {
            dependent_relationship  
        };

        // Add the parameter OID to the list of table parameter OIDs, so no duplicate statements are added
        let join: Join = Join {
            table_oid
        };
        self.joins.insert(param_oid, join.clone());

        // Add the join statement
        self.insert_join(join_statement);

        // Add a constant to indicate the parameter's associated table
        self.insert_col(format!("{table_oid} AS r{param_oid}_TABLE_OID"));
        // Add a column for the OID of the parameter's associated row OID
        self.insert_col(format!("r{param_oid}.OID AS r{param_oid}_OID"));
        return Ok(join);
    }
}