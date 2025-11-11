defmodule Ecto.Adapters.Adbc.Connection do
  @moduledoc false

  @behaviour Ecto.Adapters.SQL.Connection

  alias Ecto.Migration.{Index, Table}
  alias Ecto.Query.{ByExpr, JoinExpr, QueryExpr, WithExpr}

  import Ecto.Adapters.Adbc.DataType

  defp default_opts(opts) do
    opts
    |> Keyword.put_new(:pool_size, 5)
  end

  def start_link(opts) do
    opts = default_opts(opts)
    DBConnection.start_link(Adbcex.Connection, opts)
  end

  @impl true
  def child_spec(options) do
    {:ok, _} = Application.ensure_all_started(:db_connection)
    options = default_opts(options)
    DBConnection.child_spec(Adbcex.Connection, options)
  end

  @impl true
  def prepare_execute(conn, name, sql, params, options) do
    query = %Adbcex.Query{name: name, statement: sql}

    case DBConnection.prepare_execute(conn, query, params, options) do
      {:ok, _, _} = ok -> ok
      {:error, %Adbcex.Error{}} = error -> error
      {:error, err} -> raise err
    end
  end

  @impl true
  def execute(conn, %Adbcex.Query{ref: ref} = cached, params, options)
      when ref != nil do
    DBConnection.execute(conn, cached, params, options)
  end

  def execute(conn, %Adbcex.Query{statement: statement, ref: nil}, params, options) do
    execute(conn, statement, params, options)
  end

  def execute(conn, sql, params, options) when is_binary(sql) or is_list(sql) do
    query = %Adbcex.Query{name: "", statement: IO.iodata_to_binary(sql)}

    case DBConnection.prepare_execute(conn, query, params, options) do
      {:ok, %Adbcex.Query{}, result} -> {:ok, result}
      {:error, %Adbcex.Error{}} = error -> error
      {:error, err} -> raise err
    end
  end

  def execute(conn, query, params, options) do
    case DBConnection.execute(conn, query, params, options) do
      {:ok, _} = ok -> ok
      {:error, %ArgumentError{} = err} -> {:reset, err}
      {:error, %Adbcex.Error{}} = error -> error
      {:error, err} -> raise err
    end
  end

  @impl true
  def query(conn, sql, params, options) do
    query = %Adbcex.Query{statement: IO.iodata_to_binary(sql)}

    case DBConnection.execute(conn, query, params, options) do
      {:ok, _, result} -> {:ok, result}
      other -> other
    end
  end

  @impl true
  def query_many(_conn, _sql, _params, _opts) do
    raise RuntimeError, "query_many is not supported in the ADBC adapter"
  end

  @impl true
  def stream(conn, sql, params, options) do
    query = %Adbcex.Query{statement: sql}
    DBConnection.stream(conn, query, params, options)
  end

  @impl true
  def to_constraints(%Adbcex.Error{message: "UNIQUE constraint failed: index " <> constraint}, _opts) do
    [unique: String.trim(constraint, ~s('))]
  end

  def to_constraints(%Adbcex.Error{message: "UNIQUE constraint failed: " <> constraint}, _opts) do
    [unique: constraint]
  end

  def to_constraints(%Adbcex.Error{message: "FOREIGN KEY constraint failed"}, _opts) do
    [foreign_key: nil]
  end

  def to_constraints(%Adbcex.Error{message: "CHECK constraint failed: " <> name}, _opts) do
    [check: name]
  end

  def to_constraints(_, _), do: []

  ##
  ## Queries
  ##

  @impl true
  def all(%Ecto.Query{lock: lock}) when lock != nil do
    raise ArgumentError, "locks are not supported by DuckDB via ADBC"
  end

  def all(query, as_prefix \\ []) do
    sources = create_names(query, as_prefix)

    cte = cte(query, sources)
    from = from(query, sources)
    select = select(query, sources)
    join = join(query, sources)
    where = where(query.wheres, sources)
    group_by = group_by(query, sources)
    having = having(query, sources)
    window = window(query, sources)
    combinations = combinations(query, as_prefix)
    order_by = order_by(query, sources)
    limit = limit(query, sources)
    offset = offset(query, sources)

    [
      cte,
      select,
      from,
      join,
      where,
      group_by,
      having,
      window,
      combinations,
      order_by,
      limit,
      offset
    ]
  end

  @impl true
  def update_all(query, prefix \\ nil) do
    %{from: %{source: source}} = query
    sources = nil

    {from, name} = get_source(query, sources, 0, source)
    fields = update_fields(query, sources)
    {join, wheres} = using_join(query, :update_all, "FROM", sources)

    [
      "UPDATE ",
      quote_table(prefix, from),
      " AS ",
      quote_name(name),
      " SET ",
      fields,
      join,
      wheres | returning(query, sources, "RETURNING")
    ]
  end

  @impl true
  def delete_all(query, prefix \\ nil) do
    %{from: %{source: source}} = query

    {from, name} = get_source(query, sources = nil, 0, source)
    {join, wheres} = using_join(query, :delete_all, "USING", sources)

    [
      "DELETE FROM ",
      quote_table(prefix, from),
      " AS ",
      quote_name(name),
      join,
      wheres | returning(query, sources, "RETURNING")
    ]
  end

  @impl true
  def insert(prefix, table, header, rows, on_conflict, returning, _placeholders) do
    fields = quote_names(header)

    [
      "INSERT INTO ",
      quote_table(prefix, table),
      " (",
      fields,
      ") ",
      insert_all(rows, 1),
      on_conflict(on_conflict, header) | returning(returning, "RETURNING")
    ]
  end

  @impl true
  def update(prefix, table, fields, filters, returning) do
    {fields, count} = Enum.map_reduce(fields, 1, fn field, acc ->
      {[quote_name(field), " = $", Integer.to_string(acc)], acc + 1}
    end)

    {filters, _count} = Enum.map_reduce(filters, count, fn field, acc ->
      {[quote_name(field), " = $", Integer.to_string(acc)], acc + 1}
    end)

    [
      "UPDATE ",
      quote_table(prefix, table),
      " SET ",
      Enum.intersperse(fields, ", "),
      " WHERE ",
      Enum.intersperse(filters, " AND ") | returning(returning, "RETURNING")
    ]
  end

  @impl true
  def delete(prefix, table, filters, returning) do
    {filters, _} = Enum.map_reduce(filters, 1, fn field, acc ->
      {[quote_name(field), " = $", Integer.to_string(acc)], acc + 1}
    end)

    [
      "DELETE FROM ",
      quote_table(prefix, table),
      " WHERE ",
      Enum.intersperse(filters, " AND ") | returning(returning, "RETURNING")
    ]
  end

  @impl true
  def explain_query(conn, query, params, opts) do
    {sql_query, params_names} =
      case query do
        %Ecto.Query{} = ecto_query ->
          {all(ecto_query, :raise_on_subquery), []}

        statement when is_binary(statement) ->
          {statement, params}
      end

    explain_opts = [analyze: false, verbose: false, settings: false, optimizer: false]
    explain_opts = Keyword.merge(explain_opts, opts)
    parts = ["EXPLAIN"]

    parts =
      if explain_opts[:analyze], do: parts ++ ["ANALYZE"], else: parts

    query_string = [Enum.intersperse(parts, " "), " ", sql_query]

    case query(conn, IO.iodata_to_binary(query_string), params_names, []) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map_join(rows, "\n", & &1)}
      error -> error
    end
  end

  @impl true
  def table_exists_query(table) do
    {"SELECT 1 FROM information_schema.tables WHERE table_name = $1 LIMIT 1", [table]}
  end

  @impl true
  def ddl_logs(_result), do: []

  ## DDL

  @impl true
  def execute_ddl({command, %Table{} = table, columns}) when command in [:create, :create_if_not_exists] do
    # Create sequences for serial columns first
    sequences = create_sequences_for_table(table, columns)

    table_query = [
      if_do(command == :create_if_not_exists, "CREATE TABLE IF NOT EXISTS ", "CREATE TABLE "),
      quote_table(table.prefix, table.name),
      ?\s,
      ?(,
      column_definitions(table, columns),
      ?),
      options_expr(table.options)
    ]

    sequences ++ [table_query]
  end

  def execute_ddl({:drop, %Table{} = table, mode}) do
    [
      [
        "DROP TABLE ",
        if_do(mode == :cascade, "IF EXISTS ", ""),
        quote_table(table.prefix, table.name)
      ]
    ]
  end

  def execute_ddl({:drop_if_exists, %Table{} = table, mode}) do
    [
      [
        "DROP TABLE IF EXISTS ",
        quote_table(table.prefix, table.name),
        if_do(mode == :cascade, " CASCADE", "")
      ]
    ]
  end

  def execute_ddl({:alter, %Table{} = table, changes}) do
    Enum.map(changes, fn change ->
      ["ALTER TABLE ", quote_table(table.prefix, table.name), ?\s, column_change(table, change)]
    end)
  end

  def execute_ddl({:create, %Index{} = index}) do
    fields = Enum.map_intersperse(index.columns, ", ", &index_expr/1)

    queries = [
      [
        "CREATE ",
        if_do(index.unique, "UNIQUE ", ""),
        "INDEX ",
        if_do(index.concurrently, "CONCURRENTLY ", ""),
        quote_name(index.name),
        " ON ",
        quote_table(index.prefix, index.table),
        ?\s,
        ?(,
        fields,
        ?),
        if_do(index.where, [" WHERE ", to_string(index.where)], "")
      ]
    ]

    queries
  end

  def execute_ddl({:create_if_not_exists, %Index{} = index}) do
    # DuckDB doesn't support IF NOT EXISTS for indexes, just create it
    execute_ddl({:create, index})
  end

  def execute_ddl({:drop, %Index{} = index, mode}) do
    [
      [
        "DROP INDEX ",
        if_do(index.concurrently, "CONCURRENTLY ", ""),
        if_do(mode == :cascade, "IF EXISTS ", ""),
        quote_table(index.prefix, index.name)
      ]
    ]
  end

  def execute_ddl({:drop_if_exists, %Index{} = index, mode}) do
    [
      [
        "DROP INDEX IF EXISTS ",
        if_do(index.concurrently, "CONCURRENTLY ", ""),
        quote_table(index.prefix, index.name),
        if_do(mode == :cascade, " CASCADE", "")
      ]
    ]
  end

  def execute_ddl({:rename, %Table{} = current_table, %Table{} = new_table}) do
    [
      [
        "ALTER TABLE ",
        quote_table(current_table.prefix, current_table.name),
        " RENAME TO ",
        quote_table(new_table.prefix, new_table.name)
      ]
    ]
  end

  def execute_ddl({:rename, %Table{} = table, old_col, new_col}) do
    [
      [
        "ALTER TABLE ",
        quote_table(table.prefix, table.name),
        " RENAME COLUMN ",
        quote_name(old_col),
        " TO ",
        quote_name(new_col)
      ]
    ]
  end

  def execute_ddl(string) when is_binary(string), do: [string]

  def execute_ddl(keyword) when is_list(keyword) do
    raise ArgumentError, "DuckDB adapter does not support keyword lists in execute_ddl"
  end

  ## Helpers

  defp create_sequences_for_table(table, columns) do
    Enum.flat_map(columns, fn
      {:add, name, type, opts} ->
        pk = Keyword.get(opts, :primary_key)
        default = Keyword.fetch(opts, :default)
        auto_increment = type in [:id, :serial, :bigserial] && pk && default == :error

        if auto_increment do
          seq_name = "#{table.name}_#{name}_seq"
          [["CREATE SEQUENCE IF NOT EXISTS ", seq_name]]
        else
          []
        end

      _ ->
        []
    end)
  end

  defp column_definitions(table, columns) do
    Enum.map_intersperse(columns, ", ", &column_definition(table, &1))
  end

  defp column_definition(table, {:add, name, type, opts}) do
    [
      quote_name(name),
      ?\s,
      column_type(type, opts),
      column_options(table, name, type, opts)
    ]
  end

  defp column_change(_table, {:add, name, type, opts}) do
    ["ADD COLUMN ", quote_name(name), ?\s, column_type(type, opts), column_options(opts)]
  end

  defp column_change(_table, {:modify, name, type, opts}) do
    ["ALTER COLUMN ", quote_name(name), " TYPE ", column_type(type, opts)]
  end

  defp column_change(_table, {:remove, name, _type, _opts}) do
    ["DROP COLUMN ", quote_name(name)]
  end

  defp column_change(_table, {:remove, name}) do
    ["DROP COLUMN ", quote_name(name)]
  end

  defp column_type({:array, type}, opts), do: column_type(type, opts)
  defp column_type(type, opts), do: type_to_db(type, opts)

  defp column_options(table, name, type, opts) do
    default = Keyword.fetch(opts, :default)
    null = Keyword.get(opts, :null)
    pk = Keyword.get(opts, :primary_key)

    # For serial types with primary key and no explicit default, use sequence
    auto_increment = type in [:id, :serial, :bigserial] && pk && default == :error

    [
      if_do(null == false, " NOT NULL", ""),
      if_do(auto_increment, sequence_default(table, name), default_expr(default, type)),
      if_do(pk, [" PRIMARY KEY", pk_definition(table, [name], opts)], "")
    ]
  end

  defp sequence_default(table, column) do
    # Use a sequence for auto-increment IDs
    seq_name = "#{table.name}_#{column}_seq"
    " DEFAULT nextval('#{seq_name}')"
  end

  defp column_options(opts) do
    default = Keyword.fetch(opts, :default)
    null = Keyword.get(opts, :null)

    [
      if_do(null == false, " NOT NULL", ""),
      default_expr(default, nil)
    ]
  end

  defp pk_definition(_table, _columns, _opts), do: []

  defp default_expr({:ok, nil}, _type), do: " DEFAULT NULL"
  defp default_expr({:ok, literal}, _type) when is_binary(literal), do: [" DEFAULT '", escape_string(literal), ?']
  defp default_expr({:ok, literal}, _type) when is_number(literal), do: [" DEFAULT ", to_string(literal)]
  defp default_expr({:ok, literal}, _type) when is_boolean(literal), do: [" DEFAULT ", to_string(if literal, do: 1, else: 0)]
  defp default_expr({:ok, {:fragment, expr}}, _type), do: [" DEFAULT ", expr]
  defp default_expr(:error, _), do: []

  defp index_expr(literal) when is_binary(literal), do: literal
  defp index_expr(literal), do: quote_name(literal)

  defp options_expr(nil), do: []
  defp options_expr([]), do: []
  defp options_expr(keyword) when is_list(keyword), do: []

  defp on_conflict({:raise, _, []}, _header), do: []
  defp on_conflict({:nothing, _, []}, _header), do: " ON CONFLICT DO NOTHING"

  defp on_conflict({:replace_all, _, conflict_target}, header) do
    [" ON CONFLICT ", conflict_target_sql(conflict_target), " DO UPDATE SET " | replace_all_fields(header)]
  end

  defp on_conflict({:replace, fields, conflict_target}, _header) do
    [" ON CONFLICT ", conflict_target_sql(conflict_target), " DO UPDATE SET " | replace_fields(fields)]
  end

  defp on_conflict({fields, conflict_target}, _header) when is_list(fields) do
    [" ON CONFLICT ", conflict_target_sql(conflict_target), " DO UPDATE SET " | replace_fields(fields)]
  end

  defp conflict_target_sql([]), do: []
  defp conflict_target_sql(targets), do: ["(", Enum.map_intersperse(targets, ", ", &quote_name/1), ")"]

  defp replace_all_fields(header) do
    Enum.map_intersperse(header, ", ", fn field ->
      quoted = quote_name(field)
      [quoted, " = ", "EXCLUDED.", quoted]
    end)
  end

  defp replace_fields(fields) do
    Enum.map_intersperse(fields, ", ", fn field ->
      quoted = quote_name(field)
      [quoted, " = ", "EXCLUDED.", quoted]
    end)
  end

  defp using_join(%{joins: [], wheres: wheres}, _kind, _prefix, sources) do
    {[], where(wheres, sources)}
  end

  defp using_join(%{joins: joins, wheres: wheres} = query, kind, prefix, sources) do
    froms =
      Enum.map_intersperse(joins, ", ", fn
        %JoinExpr{qual: :inner, ix: ix, source: source} ->
          {join, name} = get_source(query, sources, ix, source)
          [quote_table(nil, join), " AS ", quote_name(name)]

        %JoinExpr{qual: qual} ->
          error!(nil, "DuckDB supports only inner joins on #{kind}, got: `#{qual}`")
      end)

    wheres = Enum.map(joins, & &1.on) ++ wheres

    {[?\s, prefix, ?\s | froms], where(wheres, sources)}
  end

  defp update_fields(%{updates: updates} = query, sources) do
    fields =
      for(
        %{expr: expr} <- updates,
        {op, kw} <- expr,
        {key, value} <- kw,
        do: {op, key, value}
      )

    {fields, _} =
      Enum.map_reduce(fields, length(all_bindings(query)), fn {op, key, value}, acc ->
        {[quote_name(key), " = " | update_op(op, value, sources, acc)], acc + 1}
      end)

    Enum.intersperse(fields, ", ")
  end

  defp update_op(:set, value, sources, acc) do
    [expr(value, sources, acc)]
  end

  defp update_op(:inc, value, sources, acc) do
    [quote_name(value), " + ", expr(value, sources, acc)]
  end

  defp update_op(other, _value, _sources, _acc) do
    error!(nil, "Unknown update operation #{inspect(other)}")
  end

  defp insert_all(rows, counter) when is_list(rows) do
    {formatted, _} =
      Enum.map_reduce(rows, counter, fn row, acc ->
        {vals, acc} =
          Enum.map_reduce(row, acc, fn
            nil, acc -> {"DEFAULT", acc}
            _, acc -> {"?", acc + 1}
          end)

        {"(#{Enum.join(vals, ", ")})", acc}
      end)

    "VALUES #{Enum.join(formatted, ", ")}"
  end

  defp returning([], _prefix), do: []

  defp returning(returning, prefix) do
    [?\s, prefix, ?\s | Enum.map_intersperse(returning, ", ", &quote_name/1)]
  end

  defp returning(%{select: nil}, _sources, _prefix), do: []

  defp returning(%{select: %{fields: fields}} = query, sources, prefix) do
    [?\s, prefix, ?\s | select_fields(fields, sources, query)]
  end

  defp create_names(%{prefix: prefix, sources: sources}, as_prefix) do
    create_names(sources, 0, tuple_size(sources), as_prefix, prefix) |> List.to_tuple()
  end

  defp create_names(sources, pos, limit, as_prefix, prefix) when pos < limit do
    current =
      case elem(sources, pos) do
        {:fragment, _, _} ->
          {nil, as_prefix ++ [?f | Integer.to_string(pos)], nil}

        {table, schema, _} ->
          name = as_prefix ++ [create_alias(table) | Integer.to_string(pos)]
          {quote_table(prefix, table), name, schema}

        %Ecto.SubQuery{} ->
          {nil, as_prefix ++ [?s | Integer.to_string(pos)], nil}
      end

    [current | create_names(sources, pos + 1, limit, as_prefix, prefix)]
  end

  defp create_names(_sources, pos, pos, _as_prefix, _prefix) do
    []
  end

  defp create_alias(<<first, _rest::binary>>), do: first
  defp create_alias(""), do: ?t

  defp get_source(query, sources, ix, source) do
    {expr, name, _schema} =
      case sources do
        nil -> get_source_from_query(query, ix, source)
        _ -> elem(sources, ix)
      end

    {expr || expr(source, sources, ix), name}
  end

  defp get_source_from_query(%{sources: sources}, ix, _source) do
    elem(sources, ix)
  end

  defp cte(%{with_ctes: %WithExpr{queries: [_ | _] = queries}}, sources) do
    ctes =
      Enum.map_intersperse(queries, ", ", fn {name, cte} ->
        [quote_name(name), " AS ", cte_query(cte, sources)]
      end)

    ["WITH ", ctes, ?\s]
  end

  defp cte(%{with_ctes: _}, _), do: []

  defp cte_query(%Ecto.Query{} = query, sources) do
    [?(, all(query, subquery_as_prefix(sources)), ?)]
  end

  defp cte_query(%QueryExpr{expr: expr}, sources) do
    expr(expr, sources, 0)
  end

  defp from(%{from: %{source: source, hints: hints}} = query, sources) do
    {from, name} = get_source(query, sources, 0, source)
    ["FROM ", from, " AS ", quote_name(name) | Enum.map(hints, &[?\s | &1])]
  end

  defp select(%{select: %{fields: fields}} = query, sources) do
    ["SELECT ", select_fields(fields, sources, query)]
  end

  defp select_fields(fields, sources, query) do
    Enum.map_intersperse(fields, ", ", fn
      {key, value} ->
        [expr(value, sources, query), " AS ", quote_name(key)]

      value ->
        expr(value, sources, query)
    end)
  end

  defp join(%{joins: []}, _sources), do: []

  defp join(%{joins: joins} = query, sources) do
    Enum.map(joins, fn
      %JoinExpr{on: %QueryExpr{expr: expr}, qual: qual, ix: ix, source: source, hints: hints} ->
        {join, name} = get_source(query, sources, ix, source)

        [
          join_qual(qual),
          join,
          " AS ",
          quote_name(name),
          Enum.map(hints, &[?\s | &1]) | join_on(qual, expr, sources)
        ]
    end)
  end

  defp join_on(:cross, true, _sources), do: []
  defp join_on(:cross, expr, _sources), do: error!(nil, "DuckDB supports only `on: true` for cross joins, got: #{inspect(expr)}")
  defp join_on(_kind, expr, sources), do: [" ON " | expr(expr, sources, 0)]

  defp join_qual(:inner), do: " INNER JOIN "
  defp join_qual(:left), do: " LEFT JOIN "
  defp join_qual(:right), do: " RIGHT JOIN "
  defp join_qual(:full), do: " FULL JOIN "
  defp join_qual(:cross), do: " CROSS JOIN "

  defp where(wheres, _sources) when is_list(wheres) and length(wheres) == 0, do: []

  defp where(wheres, sources) when is_list(wheres) do
    boolean_exprs = Enum.map(wheres, &(&1.expr))
    [" WHERE " | Enum.map_intersperse(boolean_exprs, " AND ", &expr(&1, sources, 0))]
  end

  defp having([], _sources), do: []

  defp having(%{having: havings}, sources) do
    boolean_exprs = Enum.map(havings, &(&1.expr))
    [" HAVING " | Enum.map_intersperse(boolean_exprs, " AND ", &expr(&1, sources, 0))]
  end

  defp having(_, _), do: []

  defp group_by(%{group_bys: []}, _sources), do: []

  defp group_by(%{group_bys: group_bys}, sources) do
    exprs =
      Enum.flat_map(group_bys, fn %ByExpr{expr: expr} ->
        Enum.map(expr, &expr(&1, sources, 0))
      end)

    [" GROUP BY " | Enum.intersperse(exprs, ", ")]
  end

  defp window(%{windows: []}, _sources), do: []

  defp window(%{windows: windows}, sources) do
    Enum.map(windows, fn {name, %{expr: kw}} ->
      {partition_by, order_by} = Enum.split_with(kw, &match?({:partition_by, _}, &1))

      [
        " WINDOW ",
        quote_name(name),
        " AS (",
        window_partition_by(partition_by, sources),
        window_order_by(order_by, sources),
        ?)
      ]
    end)
  end

  defp window_partition_by([], _sources), do: []

  defp window_partition_by([{:partition_by, fields}], sources) do
    ["PARTITION BY " | Enum.map_intersperse(fields, ", ", &expr(&1, sources, 0))]
  end

  defp window_order_by([], _sources), do: []

  defp window_order_by([{:order_by, fields}], sources) do
    [" ORDER BY " | Enum.map_intersperse(fields, ", ", &order_by_expr(&1, sources))]
  end

  defp order_by(%{order_bys: []}, _sources), do: []

  defp order_by(%{order_bys: order_bys}, sources) do
    exprs =
      Enum.flat_map(order_bys, fn %ByExpr{expr: expr} ->
        Enum.map(expr, &order_by_expr(&1, sources))
      end)

    [" ORDER BY " | Enum.intersperse(exprs, ", ")]
  end

  defp order_by_expr({dir, expr}, sources) do
    str = expr(expr, sources, 0)

    case dir do
      :asc -> str
      :asc_nulls_last -> [str | " ASC NULLS LAST"]
      :asc_nulls_first -> [str | " ASC NULLS FIRST"]
      :desc -> [str | " DESC"]
      :desc_nulls_last -> [str | " DESC NULLS LAST"]
      :desc_nulls_first -> [str | " DESC NULLS FIRST"]
    end
  end

  defp limit(%{limit: nil}, _sources), do: []

  defp limit(%{limit: %QueryExpr{expr: expr}}, sources) do
    [" LIMIT " | expr(expr, sources, 0)]
  end

  defp offset(%{offset: nil}, _sources), do: []

  defp offset(%{offset: %QueryExpr{expr: expr}}, sources) do
    [" OFFSET " | expr(expr, sources, 0)]
  end

  defp combinations(%{combinations: combinations}, as_prefix) do
    Enum.map(combinations, fn
      {:union, query} -> [" UNION " | all(query, as_prefix)]
      {:union_all, query} -> [" UNION ALL " | all(query, as_prefix)]
      {:except, query} -> [" EXCEPT " | all(query, as_prefix)]
      {:except_all, query} -> [" EXCEPT ALL " | all(query, as_prefix)]
      {:intersect, query} -> [" INTERSECT " | all(query, as_prefix)]
      {:intersect_all, query} -> [" INTERSECT ALL " | all(query, as_prefix)]
    end)
  end

  defp expr({:^, [], [_ix]}, _sources, _query) do
    "?"
  end

  defp expr({{:., _, [{:parent_as, _, [as]}, field]}, _, []}, _sources, _query)
       when is_atom(field) do
    [quote_qualified_name(as), ?., quote_name(field)]
  end

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query)
       when is_atom(field) do
    {_, name, _} = elem(sources, idx)
    [quote_name(name), ?., quote_name(field)]
  end

  defp expr({:&, _, [idx]}, sources, _query) do
    {_source, name, _schema} = elem(sources, idx)
    quote_name(name)
  end

  defp expr({:in, _, [left, right]}, sources, query) do
    [expr(left, sources, query), " IN ", expr(right, sources, query)]
  end

  defp expr({:is_nil, _, [arg]}, sources, query) do
    [expr(arg, sources, query), " IS NULL"]
  end

  defp expr({:not, _, [expr]}, sources, query) do
    ["NOT (", expr(expr, sources, query), ?)]
  end

  defp expr({:filter, _, [agg, filter]}, sources, query) do
    ["(", expr(agg, sources, query), ") FILTER (WHERE ", expr(filter, sources, query), ")"]
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map(parts, fn
      {:raw, str} -> str
      {:expr, expr} -> expr(expr, sources, query)
    end)
  end

  defp expr({:datetime_add, _, [datetime, count, interval]}, sources, query) do
    ["(", expr(datetime, sources, query), " + INTERVAL ", expr(count, sources, query), " ", interval_to_sql(interval), ?)]
  end

  defp expr({:date_add, _, [date, count, interval]}, sources, query) do
    ["(", expr(date, sources, query), " + INTERVAL ", expr(count, sources, query), " ", interval_to_sql(interval), ?)]
  end

  defp expr({:over, _, [agg, name]}, sources, query) when is_atom(name) do
    [expr(agg, sources, query), " OVER ", quote_name(name)]
  end

  defp expr({:over, _, [agg, kw]}, sources, query) do
    {partition_by, order_by} = Enum.split_with(kw, &match?({:partition_by, _}, &1))

    [
      expr(agg, sources, query),
      " OVER (",
      window_partition_by(partition_by, sources),
      window_order_by(order_by, sources),
      ?)
    ]
  end

  defp expr({:{}, _, elems}, sources, query) do
    ["(", Enum.map_intersperse(elems, ", ", &expr(&1, sources, query)), ")"]
  end

  defp expr({:count, _, []}, _sources, _query), do: "COUNT(*)"

  defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    {modifier, args} =
      case args do
        [rest, :distinct] -> {"DISTINCT ", [rest]}
        _ -> {"", args}
      end

    [
      Atom.to_string(fun),
      ?(,
      modifier,
      Enum.map_intersperse(args, ", ", &expr(&1, sources, query)),
      ?)
    ]
  end

  defp expr(%Ecto.SubQuery{query: query}, _sources, _query) do
    [?(, all(query, subquery_as_prefix(nil)), ?)]
  end

  defp expr(nil, _sources, _query), do: "NULL"
  defp expr(true, _sources, _query), do: "1"
  defp expr(false, _sources, _query), do: "0"

  defp expr(literal, _sources, _query) when is_binary(literal) do
    [?', escape_string(literal), ?']
  end

  defp expr(literal, _sources, _query) when is_integer(literal) do
    Integer.to_string(literal)
  end

  defp expr(literal, _sources, _query) when is_float(literal) do
    Float.to_string(literal)
  end

  defp expr(literal, _sources, _query) when is_list(literal) do
    ["(", Enum.map_intersperse(literal, ", ", &expr(&1, _sources = nil, _query = nil)), ")"]
  end

  defp interval_to_sql(:year), do: "YEAR"
  defp interval_to_sql(:month), do: "MONTH"
  defp interval_to_sql(:week), do: "WEEK"
  defp interval_to_sql(:day), do: "DAY"
  defp interval_to_sql(:hour), do: "HOUR"
  defp interval_to_sql(:minute), do: "MINUTE"
  defp interval_to_sql(:second), do: "SECOND"
  defp interval_to_sql(:millisecond), do: "MILLISECOND"
  defp interval_to_sql(:microsecond), do: "MICROSECOND"

  defp quote_name(name) when is_atom(name), do: quote_name(Atom.to_string(name))
  defp quote_name(name) when is_list(name), do: quote_name(IO.iodata_to_binary(name))
  defp quote_name(name) when is_binary(name) do
    if String.contains?(name, "\"") do
      error!(nil, "bad field name #{inspect(name)}")
    end

    [?", name, ?"]
  end

  defp quote_names(names), do: Enum.map_intersperse(names, ", ", &quote_name/1)

  defp quote_table(nil, name), do: quote_table(name)
  defp quote_table(prefix, name), do: [quote_table(prefix), ?., quote_table(name)]

  defp quote_table(name) when is_atom(name), do: quote_table(Atom.to_string(name))
  defp quote_table(name) do
    if String.contains?(name, "\"") do
      error!(nil, "bad table name #{inspect(name)}")
    end

    [?", name, ?"]
  end

  defp quote_qualified_name(name) when is_atom(name), do: quote_qualified_name(Atom.to_string(name))
  defp quote_qualified_name(name), do: [?", name, ?"]

  defp escape_string(value) when is_binary(value) do
    String.replace(value, "'", "''")
  end

  defp if_do(condition, value, _else) when condition, do: value
  defp if_do(_condition, _value, else_value), do: else_value

  defp all_bindings(%{select: %{expr: expr}}) do
    all_bindings(expr, 0)
  end

  defp all_bindings({:&, _, [ix]}, max), do: max(ix, max)
  defp all_bindings({left, right}, max), do: max(all_bindings(left, max), all_bindings(right, max))
  defp all_bindings({left, _, right}, max), do: max(all_bindings(left, max), all_bindings(right, max))
  defp all_bindings(list, max) when is_list(list), do: Enum.reduce(list, max, &max(all_bindings(&1, &2), &2))
  defp all_bindings(_, max), do: max

  defp subquery_as_prefix(sources) when is_tuple(sources), do: [?s]
  defp subquery_as_prefix(_), do: []

  defp error!(nil, message) do
    raise ArgumentError, message
  end

  defp error!(query, message) do
    raise Ecto.QueryError, query: query, message: message
  end
end
