# DungeonDB

DungeonDB is a spreadsheet-like GUI for creating relational database schemas and inputting data, inspired by applications such as Microsoft Access, LibreOffice Base, and [CastleDB](http://castledb.org/).

There are certain features that the listed inspirations touch on but do not fully go all the way with, which is the space that DungeonDB means to fill. The major goals of the project are as follows:
- A modern, responsive UI.
- Linux compatibility.
- Schemas that are able to inherit columns from other schemas, for object polymorphism.
- Build reports by leveraging the relationships between tables.
- Easy export into formats that can be read by other programs, such as JSON or XML.

## Features

There are two kinds of schemas in DungeonDB: tables and reports. 

Tables are concrete schemas where data is stored. The possible types of columns on a table are:
- `Plain Text`: A basic text entry.
- `Checkbox`: A checkbox.
- `Integer`: An entry for any whole number.
- `Number`: An entry for any number.
- `Date`: A picker for a date.
- `Datetime`: A picker for a date and time.
- `Object`: A link to the Object view of a unique row in another table. The linked row is unset by default. If the linked row is unset, then clicking on the field creates a new row in the other table.
- `Single-Select Dropdown`: A dropdown field that lets the user select a row from another table.
- `Multi-Select Dropdown`: Same as above, but allows multiple rows to be selected.
- `File`: Allows the user to upload or link to a file.
- `Image`: Same as above, but the image will be displayed if possible.
- `JSON`: Text entry that must conform to JSON standard.
- `Formula`: A virtual field that evaluates a formula. If the formula's return value is the unmodified value of another cell, then the field may be used to edit that cell; otherwise, the field will be read-only. For more information, see [Formulas](docs/Formulas.md).
- `Drill-Through Report`: A virtual field that links to a report with the current filters of the row applied.

On the other hand, reports are virtual schemas. The only types of column allowed for a report are `Formula` and `Drill-Through Report`.

### Inheritance and Subtyping

Reports can inherit columns from other reports, and tables can inherit columns both from reports and from other tables. Both reports and tables can inherit from multiple different sources.

For tables inheriting from other tables, the way that this works is that an associated row is created in the table that is being inherited from. So if you had tables A, B, C, with tables B and C both inheriting from A, then A will have a row for every row of B, every row of C, and additionally may have rows that belong to neither B nor C. An example is as follows:

| A | Key || B | Key | B-Only Value || C | Key | C-Only Value |
|---|---|---|---|---|---|---|---|---|---|
|| a1 |
|| b1 ||| b1 | This is the B-only value associated with b1. | 
|| b2 ||| b2 | This is the B-only value associated with b2. |
|| c1 ||||||| c1 | This is the C-only value associated with c1. |
|| a2 |
|| b3 ||| b3 | This is the B-only value associated with b3. |
|| c2 ||||||| c2 | This is the C-only value associated with c2. |

Note how there are no rows in A associated with both B *and* C. This is because each row of a table has to have a single ending for its inheritance chain; if a row in A were associated with B and C, then that row would have multiple endings for its inheritance chain, which is not allowed. However, if there was a table D that inherited from both B and C, then a row in A could be associated with both B and C as long as it were also associated with D. An example is as follows:

| A | Key || B | Key | B-Only Value || C | Key | C-Only Value || D | Key | B-Only Value | C-Only Value |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|| a1 |
|| b1 ||| b1 | This is the B-only value associated with b1. | 
|| b2 ||| b2 | This is the B-only value associated with b2. |
|| c1 ||||||| c1 | This is the C-only value associated with c1. |
|| a2 |
|| b3 ||| b3 | This is the B-only value associated with b3. |
|| c2 ||||||| c2 | This is the C-only value associated with c2. |
|| d1 ||| d1 | This is the B-only value associated with d1. ||| d1 | This is the C-only value associated with d1. ||| d1 | This is the B-only value associated with d1. | This is the C-only value associated with d1. |

The singular ending for the inheritance chain for a row is known as the "subtype" of that row. The Object view for a row in a table displays the columns for that row's subtype, and also allows the user to change what the subtype of that row is.

## Technical Details

DungeonDB is a [Tauri](https://v2.tauri.app/) app, whose Rust backend reads and writes data from SQLite database files. Although DungeonDB's .dndb file format utilizes specific naming structures for its tables, it is fundamentally an SQLite database file and can be queried and interacted with by any program that works with SQLite files (such as [DB Browser](https://sqlitebrowser.org/)). Manually writing to a .dndb file is not recommended, as it may create violations of naming, data typing, or data relational constraints.