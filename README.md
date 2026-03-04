# cpp-query-engine

A lightweight, header-based columnar query engine written in C++20. It provides SQL-like operations — filtering, projection, grouping with aggregation, sorting, and limiting — over in-memory columnar tables, with built-in CSV ingestion.

## Overview

`cpp-query-engine` models tabular data as a collection of typed columns (`INT64`, `DOUBLE`, `STRING`) and executes structured queries against them in a fixed pipeline order:

```
WHERE → GROUP BY → SELECT → ORDER BY → LIMIT
```

Queries are composed declaratively by assembling clause structs into a `Query` object and passing it to `qe::execute()`. There is no SQL parser; queries are constructed programmatically in C++.

## Project Structure

```
cpp-query-engine/
├── include/engine/
│   ├── column.hpp      # Column type with typed storage vectors
│   ├── table.hpp       # Table: named collection of columns
│   ├── csv.hpp         # CSV loader with automatic type inference
│   ├── operators.hpp   # Relational operators (filter, project, order_by, group_by, limit)
│   └── engine.hpp      # Query struct and execute() entry point
├── src/
│   ├── column.cpp      # Column append/size implementations
│   ├── table.cpp       # Table row counting and column management
│   ├── csv.cpp         # CSV parsing and type detection
│   ├── operators.cpp   # Operator implementations
│   ├── engine.cpp      # Query execution pipeline
│   └── main.cpp        # Example: query an employee dataset
├── tests/
│   ├── test_storage.cpp # Unit tests for all components
│   ├── test.csv         # Small test fixture (2 rows, 2 columns)
│   └── employees.csv    # 14-row employee dataset (department, salary, name)
└── Makefile
```

## Building

Requires a C++20-compatible compiler (e.g. GCC 10+, Clang 12+).

```bash
# Build the main executable
make

# Build and run tests
make test

# Clean build artifacts
make clean
```

The main binary is output as `qe_main` and the test binary as `qe_tests`.

## Core Concepts

### Column

A column stores its data in one of three typed vectors depending on its `Type`:

| Type     | Storage field | C++ type              |
|----------|---------------|-----------------------|
| `INT64`  | `i64`         | `std::vector<int64_t>`|
| `DOUBLE` | `f64`         | `std::vector<double>` |
| `STRING` | `str`         | `std::vector<std::string>` |

Appending a value of the wrong type throws a `std::logic_error`. A `valid` bitmap vector is declared for future nullable support but is not currently used.

### Table

A `Table` is a pair of parallel vectors: `names` (column names) and `cols` (Column objects). Adding a column enforces that its row count matches the existing table (or is the first column), and that the column name is unique.

### CSV Loading

`qe::load_csv(path, delimiter, has_header)` reads a CSV file and automatically infers column types by attempting to parse each cell as an integer first, then a double, falling back to string. Null values (`"null"`, `"Null"`, or empty cells) are skipped.

### Query Clauses

All clauses are optional and are composed into a `qe::Query`:

| Clause       | Struct             | Description |
|--------------|--------------------|-------------|
| **WHERE**    | `FilterClause`     | Filters rows by comparing a column against a constant using a predicate (`EQ`, `NEQ`, `LT`, `LE`, `GT`, `GE`). |
| **GROUP BY** | `GroupByClause`    | Groups by a key column and aggregates a value column with `COUNT`, `SUM`, `AVG`, `MIN`, or `MAX`. |
| **SELECT**   | `select_cols`      | A vector of column indices to project (keep). |
| **ORDER BY** | `OrderByClause`    | Sorts the result by a column in `ASC` or `DESC` order (stable sort). |
| **LIMIT**    | `LimitClause`      | Returns only the first _n_ rows. |

### Execution Pipeline

`qe::execute()` applies the clauses in a fixed order — filter, group, project, sort, limit — each producing a new `Table` that feeds into the next stage. Omitted clauses are simply skipped.

## Usage Example

The included `main.cpp` demonstrates a complete query against the employee dataset — equivalent to the SQL:

```sql
SELECT department, COUNT(*)
FROM employees
WHERE salary > 50
GROUP BY department
ORDER BY count DESC
LIMIT 3;
```

```cpp
qe::Table table = qe::load_csv("tests/employees.csv");

qe::Query query;
query.where    = qe::FilterClause{1, qe::Predicate::GT, 50};
query.group_by = qe::GroupByClause{0, 0, qe::AggKind::COUNT};
query.order_by = qe::OrderByClause{1, qe::SortOrder::DESC};
query.limit    = qe::LimitClause{3};

qe::Table result = qe::execute(table, query);
// Result: Engineering 3, Sales 2, HR 1
```

## Tests

The test suite (`tests/test_storage.cpp`) covers:

- **Storage**: empty tables, row counting, mismatched column sizes, type-safety enforcement
- **CSV**: loading and parsing multi-type CSV files
- **Filter**: row selection with predicate comparison
- **Project**: column subset selection
- **Order By**: descending sort with multi-column consistency
- **Group By**: all five aggregation kinds (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) on both integer and double value columns
- **Limit**: row truncation
- **Execute**: end-to-end pipeline with progressively composed queries, including a full integration test against the employee dataset
