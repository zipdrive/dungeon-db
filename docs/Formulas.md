# Formulas

## Scalar Types

`Text`: Any form of text.

`Boolean`: True or false values. The literal values of this type are written `true` and `false`.

`Number`: Floating-point numbers.

`Integer`: Whole numbers. May be used in place of a Number.

`File`: A file.

`Any`: May conform to any or none of the above types.

## Parameters

A formula may be dependent on any number of parameters. A parameter is a value that is queried from a table by means of a path. This path starts at a root table, navigates through other tables through use of references, and finally ends at a column.

For tables A and B, the possible references from table A to table B are as follows:

- If table A inherits columns directly from table B, or vice versa, then table A can reference the associated row from table B. This relationship will be one-to-one.
- Table A can reference the row of table B that is associated with an Object column on table A that points to table B. This relationship will be one-to-one.
- Table A can reference the row of table B that is selected by a Single-Select Dropdown column on table A that points to table B. This relationship will be one-to-one.
- Table A can reference the rows of table B that are selected by a Multi-Select Dropdown column on table A that points to table B. This relationship will be one-to-many.
- Table A can reference the row(s) of table B, such that an Object column on table B points back to the row on table A. This relationship will typically be one-to-one. (It is typically one-to-one because each cell of an Object column creates a new row on the referenced table. The only case where it will not be guaranteed to be one-to-one is if the Object column is originally created as a Single-Select Dropdown or Multi-Select Dropdown column, then its type is changed to Object.)
- Table A can reference the rows of table B, such that a Single-Select Dropdown column on table B points back to the row on table A. This relationship will be one-to-many.
- Table A can reference the rows of table B, such that a Multi-Select Dropdown column on table B points back to the row on table A. This relationship will be one-to-many.

## Functions

### Logic Functions

#### AND

`AND(lhs: Boolean, rhs: Boolean) -> Boolean`

The binary operator `AND` may also be used.

Returns true if both `lhs` and `rhs` are true. Otherwise, it returns false.

#### COALESCE

`COALESCE<_T: Any>(...args: _T) -> _T`

Returns the first non-null argument.

#### CONTAINS

`CONTAINS(collection: List<Any>, x: Any) -> Boolean`

The binary operator `x IN collection` may also be used.

Returns true if `x` is equal to one of the values in `collection`, and false otherwise.

#### COUNT

`COUNT(collection: List<Any>) -> Integer`

Returns the number of non-null values in `collection`.

#### EQ

`EQ(lhs: Any, rhs: Any) -> Boolean`

The binary operator `lhs = rhs` may also be used.

Returns true if `lhs` is equal to `rhs`, and false otherwise.

#### IF

`IF<_T: Any>(condition: Boolean, valueIfTrue: _T, valueIfFalse?: _T) -> _T`

If `condition` is true, this function returns `valueIfTrue`. Otherwise, it returns `valueIfFalse`.

#### INDEX

`INDEX<_T: Any>(collection: List<_T>, index: Integer) -> _T`

The indexer operator `collection[index]` may also be used.

Returns the `index`-th value of `collection`.

#### NEQ

`NEQ(lhs: Any, rhs: Any) -> Boolean`

The binary operator `lhs <> rhs` may also be used.

Returns true if `lhs` is not equal to `rhs`, and false otherwise.

#### NOT

`NOT(x: Boolean) -> Boolean`

The unary operator `NOT x` may also be used.

Returns true if `x` is false, and false if `x` is true.

#### NULLIF

`NULLIF<_T: Any>(x: _T, y: Any) -> _T`

If `x` equals `y`, then this function returns null. Otherwise, it returns `x`.

#### OR

`OR(lhs: Boolean, rhs: Boolean) -> Boolean`

The binary operator `lhs OR rhs` may also be used.

Returns true if either `lhs` or `rhs` is true. Otherwise, it returns false.

#### SWITCH

`SWITCH<_T: Any>(x: Any, ...[matchedValue: Any, returnedValue: _T], returnValueIfNoMatch?: _T) -> _T`

For the first `matchedValue` that equals `x`, this function returns the associated `returnedValue`. If no `matchedValue` arguments are equal to `x`, it returns `returnValueIfNoMatch` (or null, if `returnValueIfNoMatch` was not specified).

### Math Functions

#### ABS

`ABS(x: Number) -> Number`

Returns the absolute value of `x`.

#### ADD

`ADD<_T: Number>(lhs: _T, rhs: _T) -> _T`

The binary operator `lhs + rhs` may also be used.

Returns the addition of `lhs` and `rhs`.

#### AVG

`AVG(collection: List<Number>) -> Number`

Returns the average of all values in `collection`.

#### CEIL

`CEIL(x: Number) -> Integer`

Returns the lowest whole number greater than or equal to `x`.

#### DIVIDE

`DIVIDE(lhs: Number, rhs: Number) -> Number`

The binary operator `lhs / rhs` may also be used.

Returns `lhs` divided by `rhs`.

#### FLOOR

`FLOOR(x: Number) -> Integer`

Returns the greatest whole number less than or equal to `x`.

#### MAX

`MAX<_T: Number>(collection: List<_T>) -> _T`

Returns the maximum of all values in `collection`.

`MAX<_T: Number>(x1: _T, x2: _T, ...args: _T) -> _T`

Returns the maximum of all arguments.

#### MIN

`MIN<_T: Number>(collection: List<_T>) -> _T`

Returns the minimum of all values in `collection`.

`MIN<_T: Number>(x1: _T, x2: _T, ...args: _T) -> _T`

Returns the minimum of all arguments.

#### MODULO

`MODULO<_T: Number>(lhs: _T, rhs: _T) -> _T`

The binary operator `lhs % rhs` may also be used.

Returns the remainder when `lhs` is divided by `rhs`.

#### MULTIPLY

`MULTIPLY<_T: Number>(lhs: _T, rhs: _T) -> _T`

The binary operator `lhs * rhs` may also be used.

Returns `lhs` multiplied by `rhs`.

#### POW

`POW<_T: Number>(base: _T, exponent: _T) -> _T`

Returns `base` raised to the power of `exponent`.

#### RANDOM

`RANDOM() -> Integer`

Returns a pseudo-random integer between -9223372036854775807 and +9223372036854775807.

#### ROUND

`ROUND(x: Number) -> Integer`

Returns `x` rounded to the nearest whole number.

#### SIGN

`SIGN(x: Number) -> Integer`

Returns `+1` if `x` is greater than 0, `-1` if `x` is less than 0, and `0` if `x` is equal to 0.

#### SUBTRACT

`SUBTRACT<_T: Number>(lhs: _T, rhs: _T) -> _T`

The binary operator `lhs - rhs` may also be used.

Returns `lhs` subtracted by `rhs`.

#### SUM

`SUM<_T: Number>(collection: List<_T>) -> _T`

Returns the sum of all values in `collection`.

### Text Functions

#### CONCAT

`CONCAT(lhs: Text, rhs: Text) -> Text`

The binary operator `lhs & rhs` may also be used.

Returns the concatenation of `lhs` with `rhs`.

#### FORMAT

`FORMAT(str: Text, ...args: Any) -> Text`

Formats the arguments with the format string `str`. For more details, see the [SQLite documentation](https://sqlite.org/printf.html#formatting_details).

#### JOIN

`JOIN(collection: List<Text>, delimiter: Text) -> Text`

Returns the concatenation of all non-null values in `collection`. The concatenated values will be separated from each other by `delimiter`.

#### LENGTH

`LENGTH(x: Text) -> Integer`

Returns the number of characters in `x`.

#### LOWER

`LOWER<_T: Text>(x: _T) -> _T`

Converts `x` into lowercase characters.

#### REPLACE

`REPLACE(str: Text, pattern: Text, replacement: Text) -> Text`

Returns `str` with any instances of `pattern` replaced by `replacement`.

#### SUBSTRING

`SUBSTRING(str: Text, start: Integer, length?: Integer) -> Text`

Returns the substring of `str` starting at position `start` (with the first character in the text being position `1`). If the optional `length` argument is specified, the length of the substring will be capped at that value.

#### UPPER

`UPPER<_T: Text>(x: _T) -> _T`

Converts `x` into uppercase characters.

## Implementation Notes

When a Formula column is created or updated, the formula is converted into an equivalent SQLite expression.

In the compiled SQLite expression, a List is just an expression that has one-to-many cardinality with the rows of the schema. One consequence of this is that the items of a List must be scalar values, and that nested Lists are not possible.