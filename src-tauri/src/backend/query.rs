#[derive(Clone)]
enum Relationship {
    One,
    Many {
        intermediate_param_oid: Vec<i64>,
        final_param_oid: i64
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
    column_type: data_type::MetadataColumnType,

    /// The ordinal that can be pulled to get the true value of the column.
    true_ord: Option<String>
}

struct SelectStatement {
    /// The primary table of the query.
    base_table_oid: i64,

    /// The tables joined through a parameter.
    /// Maps from the parameter OID to the join definition.
    joins: HashMap<i64, Join>,

    /// The parameters selected by the query.
    params: HashMap<i64, Param>,

    /// The column definitions.
    cmd_cols: String,

    /// The join definitions.
    cmd_tbls: String 
}

impl SelectStatement {
    /// Creates a new statement selecting from a table OID.
    fn new(base_table_oid: i64) -> Self {
        Self {
            base_table_oid,
            joins: HashMap::new(),
            params: HashMap::new(),
            cmd_cols: format!("t.OID AS t_OID"),
            cmd_tbls: format!("FROM TABLE{base_table_oid} t")
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
    fn insert_param(&mut self, param_oid: i64) -> Result<(), error::Error> {
        // First, check to make sure the parameter hasn't already been added
        if self.params.contains_key(&param_oid) {
            return Ok(());
        }

        // Start a transaction to retrieve details of the parameter
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Retrieve all parameters it is immediately dependent on
        let (
            table_oid,
            column_oid,
            column_type_oid, 
            column_mode, 
            dependency_param_oid
        ) = trans.query_one(
            "
            WITH COLUMN_QUERY (RPT_PARAMETER_OID, COLUMN_OID, TABLE_OID, TYPE_OID, MODE, DEPENDENCY_RPT_PARAMETER_OID) AS (
                -- Links through a column in the base table
                SELECT
                    c.RPT_PARAMETER_OID,
                    c.RPT_PARAMETER_OID AS COLUMN_OID,
                    c.TABLE_OID,
                    c.TYPE_OID,
                    typ.MODE,
                    NULL AS DEPENDENCY_RPT_PARAMETER_OID
                FROM METADATA_TABLE_COLUMN c
                INNER JOIN METADATA_TYPE typ ON typ.OID = c.TYPE_OID

                UNION

                -- Chained link that terminates in a column
                SELECT
                    ch.RPT_PARAMETER_OID,
                    c.RPT_PARAMETER_OID AS COLUMN_OID,
                    c.TABLE_OID,
                    c.TYPE_OID,
                    typ.MODE,
                    ch.REF_RPT_PARAMETER_OID AS DEPENDENCY_RPT_PARAMETER_OID
                FROM METADATA_RPT_PARAMETER__CHAIN ch
                INNER JOIN METADATA_TABLE_COLUMN c ON c.RPT_PARAMETER_OID = ch.DEF_RPT_PARAMETER_OID 
                INNER JOIN METADATA_TYPE typ ON typ.OID = c.TYPE_OID 
            )
            SELECT
                TABLE_OID,
                COLUMN_OID, 
                TYPE_OID, 
                MODE, 
                DEPENDENCY_RPT_PARAMETER_OID
            FROM COLUMN_QUERY
            WHERE RPT_PARAMETER_OID = ?1
            ",
            params![param_oid],
            |row| {
                Ok((
                    row.get::<_, i64>("TABLE_OID")?,
                    row.get::<_, i64>("COLUMN_OID")?,
                    row.get::<_, i64>("TYPE_OID")?,
                    row.get::<_, i64>("MODE")?,
                    row.get::<_, Option<i64>>("DEPENDENCY_RPT_PARAMETER_OID")?
                ))
            }
        )?;
        
        // Make sure any parameter this one is dependent on is added to the query
        let relationship: Relationship;
        let source_alias: String = if let Some(o) = dependency_param_oid {
            relationship = self.insert_join_param(&trans, o)?;
            format!("p{o}")
        } else if table_oid == self.base_table_oid {
            relationship = Relationship::One;
            String::from("t")
        } else {
            return Err(error::Error::AdhocError("A report parameter does not source back to the base table."));
        };

        // Get the column type
        let column_type = data_type::MetadataColumnType::from_database(column_type_oid, column_mode);

        // Construct the query used to retrieve the parameter data
        let true_ord: Option<String> = match &column_type {
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
    fn insert_join_param(&mut self, trans: &Transaction, param_oid: i64) -> Result<Join, error::Error> {
        // First, check to make sure the parameter hasn't already been added
        if self.joins.contains_key(&param_oid) {
            return Ok(self.joins[&param_oid].clone());
        }

        // Then, make sure to add any parameter it is dependent on
        let (table_oid, dependency_param_oid, join_statement, is_many) = trans.query_one(
            "
            WITH RECURSIVE JOIN_STATEMENTS (RPT_PARAMETER_OID, TABLE_OID, DEPENDENCY_RPT_PARAMETER_OID, JOIN_STATEMENT, IS_MANY) AS (
                -- Links through a column in the base table
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
            self.insert_join_param(trans, o)?
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



pub Query {
    /// Subquery SELECT statements.
    cte: Vec<SelectStatement>,

    /// The main SELECT statement.
    main: SelectStatement
}