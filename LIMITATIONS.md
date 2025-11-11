# Ecto ADBC Adapter - Known Limitations and Unsupported Features

This document outlines the known limitations and unsupported features in the Ecto ADBC adapter for DuckDB.

## Explicitly Unsupported Features

### 1. **Cursors** ❌
**Location**: `lib/adbcex/connection.ex:104-116`

DuckDB through ADBC does not support database cursors. The following operations will return errors:
- `handle_declare/4` - Returns: `"Cursors are not supported"`
- `handle_fetch/4` - Returns: `"Cursors are not supported"`
- `handle_deallocate/4` - Returns: `"Cursors are not supported"`

**Impact**:
- No support for `Ecto.Adapters.SQL.stream/4` with cursor-based streaming
- Large result sets must be loaded into memory at once

### 2. **query_many** ❌
**Location**: `lib/ecto/adapters/adbc/connection.ex:80`

Multiple SQL statements in a single query are not supported.

**Impact**:
- Cannot execute multiple statements separated by semicolons in one query
- Each statement must be executed separately

### 3. **Advisory Locks** ❌
**Location**: `lib/ecto/adapters/adbc/connection.ex:114` and `lib/ecto/adapters/adbc.ex:46-50`

DuckDB does not support advisory locks for migration coordination.

**Impact**:
- **Migrations cannot be run concurrently** across multiple processes/nodes
- Running migrations simultaneously may cause conflicts or corruption
- The adapter runs migrations without locks (just calls the function directly)

**Recommendation**: Only run migrations from a single process.

### 4. **Keyword Lists in DDL** ❌
**Location**: `lib/ecto/adapters/adbc/connection.ex:393`

Keyword lists are not supported in `execute_ddl/1`.

**Impact**:
- Only structured DDL tuples and SQL strings are supported
- Cannot pass arbitrary keyword lists for DDL operations

### 5. **CREATE INDEX IF NOT EXISTS** ⚠️
**Location**: `lib/ecto/adapters/adbc/connection.ex:340-344`

DuckDB supports this syntax, but the adapter implementation treats `:create_if_not_exists` for indexes the same as `:create`.

**Impact**:
- Attempting to create an index that already exists will fail
- No idempotent index creation

## Connection Limitations

### 6. **Connection Pooling** ⚠️
**Location**: Configuration in `config/*.exs`

**Limitation**: DuckDB works best with `pool_size: 1` for file-based databases.

**Reason**:
- DuckDB uses file-based locking
- Multiple connections to the same file can cause contention
- In-memory databases (`:memory:`) don't have this limitation

**Recommendation**:
```elixir
config :my_app, MyApp.Repo,
  database: "path/to/db.duckdb",
  pool_size: 1  # Required for file databases
```

### 7. **In-Memory Database Limitations**
**Location**: `lib/ecto/adapters/adbc.ex:58-59, 97-98, 155-156`

In-memory databases (`:memory:`) have special handling:
- `storage_up` returns `{:error, :already_up}` (cannot create memory DB from adapter)
- `storage_down` returns `{:error, :already_down}` (cannot drop memory DB)
- `dump_cmd` returns error for memory databases

**Impact**: Memory databases must be managed differently than file databases.

## Data Type Limitations

### 8. **Date/Time Storage** ⚠️
**Location**: `lib/ecto/adapters/adbc/data_type.ex:26-33`

All date/time types are stored as **TEXT** in ISO8601 format, not as native DuckDB temporal types.

**Stored as TEXT**:
- `:date` → `TEXT`
- `:time` → `TEXT`
- `:time_usec` → `TEXT`
- `:naive_datetime` → `TEXT`
- `:naive_datetime_usec` → `TEXT`
- `:utc_datetime` → `TEXT`
- `:utc_datetime_usec` → `TEXT`

**Impact**:
- Less efficient temporal operations in the database
- Cannot use DuckDB's native temporal functions directly
- Requires string parsing for date/time operations

### 9. **JSON/Map Storage** ⚠️
**Location**: `lib/ecto/adapters/adbc/data_type.ex:58-71`

Maps and arrays are stored as **JSON-encoded TEXT** by default (configurable to BLOB).

**Impact**:
- No native JSON operations in queries
- Need to decode/encode on every read/write
- Less efficient than native JSON types

### 10. **Binary ID Storage** ⚠️
**Location**: `lib/ecto/adapters/adbc/data_type.ex:43-48`

Binary IDs and UUIDs are stored as TEXT by default (configurable to BLOB).

## Query Limitations

### 11. **Window Functions and Advanced SQL** ⚠️
While DuckDB supports advanced SQL features, some may not be properly tested with this adapter:
- Complex CTEs (Common Table Expressions)
- Advanced window functions
- PIVOT/UNPIVOT operations
- DuckDB-specific extensions

**Status**: Unknown - may work but not explicitly tested

### 12. **Fragments and Raw SQL** ⚠️
**Location**: `lib/ecto/adapters/adbc/connection.ex:486`

Fragments are supported for DEFAULT values but general fragment support in queries is limited by Ecto's SQL generation.

### 13. **Locks in Queries** ❌
**Location**: `lib/ecto/adapters/adbc/connection.ex:114`

Row-level locking (`FOR UPDATE`, `FOR SHARE`) is not supported.

```elixir
# This will raise an error:
from(u in User, lock: "FOR UPDATE")
# ArgumentError: locks are not supported by DuckDB via ADBC
```

## Migration Limitations

### 14. **Concurrent Migrations** ❌
As mentioned in #3, migrations cannot run concurrently.

### 15. **Alter Table Limitations** ⚠️
DuckDB has some limitations on ALTER TABLE operations:
- Cannot change column types in all cases
- Limited support for adding constraints to existing tables
- Some operations may require table recreation

## Testing & Production Considerations

### 16. **Ecto.Adapters.SQL.Sandbox** ⚠️
**Location**: `config/test.exs`

Sandbox mode for tests requires `pool_size: 1`:
```elixir
config :my_app, MyApp.Repo,
  database: "test.duckdb",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 1  # Required
```

### 17. **Prepared Statements** ✅
**Status**: Supported

Parameter binding with `?` placeholders is fully supported.

## Supported Features

### ✅ Fully Supported:
- Basic CRUD operations (INSERT, SELECT, UPDATE, DELETE)
- Migrations and schema management
- Transactions (BEGIN, COMMIT, ROLLBACK)
- Indexes (CREATE, DROP)
- Primary keys and auto-increment (via sequences)
- UNIQUE constraints
- FOREIGN KEY constraints (detection)
- CHECK constraints (detection)
- NOT NULL constraints
- DEFAULT values
- INSERT...RETURNING
- DDL transactions
- Query parameters and prepared statements
- Schema dumps and loads
- All basic Ecto types

## Configuration Options

### Type Storage Configuration
You can configure how certain types are stored:

```elixir
# In config.exs
config :ecto_adbc,
  binary_id_type: :string,  # or :binary
  uuid_type: :string,       # or :binary
  map_type: :string,        # or :binary
  array_type: :string,      # or :binary
  json_library: Jason       # JSON encoding library
```

## Recommendations

1. **Always use `pool_size: 1`** for file-based databases
2. **Never run concurrent migrations** - coordinate migration execution
3. **Use file databases for persistence**, memory databases for testing
4. **Be aware of TEXT storage** for dates/times/JSON when writing queries
5. **Test complex queries** before relying on them in production
6. **Use RETURNING clauses** for efficient INSERT operations
7. **Consider DuckDB limitations** when designing schema

## Future Improvements

Potential areas for enhancement:
- Native temporal type support
- Native JSON type support
- Better index creation (IF NOT EXISTS)
- Cursor/streaming support (if DuckDB adds it)
- Connection pooling improvements

## Getting Help

If you encounter issues not listed here:
1. Check DuckDB documentation for database-level limitations
2. Check ADBC library limitations
3. File an issue with reproduction steps
