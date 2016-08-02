defmodule Schemata.Query do
  @moduledoc ""

  use Schemata.CQErl
  alias Schemata.Query
  alias Schemata.Queryable
  alias Schemata.Result
  alias Schemata.CassandraError
  require Logger

  @type keyspace           :: atom | binary
  @type table              :: atom | binary
  @type column             :: atom | binary
  @type explicit_columns   :: [column]
  @type columns            :: :all | explicit_columns
  @type datatype           :: CQErl.datatype
  @type conditions         :: map
  @type consistency_level  :: CQErl.consistency_level
  @type serial_consistency :: CQErl.serial_consistency
  @type statement          :: CQErl.query_statement
  @type parameter_val      :: CQErl.parameter_val
  @type value              :: CQErl.parameter_val
  @type values             :: map
  @type limit              :: nil | pos_integer
  @type error              :: {:error, term}
  @type query_result       :: CQErl.query_result

  @type column_def         :: [{atom, datatype
                                    | {:set | :list, datatype}
                                    | {:map, datatype, datatype}}]
  @type primary_key        :: atom | [[atom] | atom]
  @type ordering           :: atom | [{atom, :asc | :desc}]
  @type ks_strategy        :: :simple | :network_topology
  @type ks_factor          :: {atom | binary, non_neg_integer}
                            | non_neg_integer

  @type t :: %Query{
    statement:            binary,
    values:               values,
    keyspace:             keyspace,
    consistency:          consistency_level,
    serial_consistency:   serial_consistency,
    reusable:             boolean,
    named:                boolean,
    page_size:            integer,
    page_state:           term,
    value_encode_handler: term
  }

  @enforce_keys [:statement]
  defstruct [
    statement:            nil,
    values:               %{},
    keyspace:             nil,
    consistency:          :quorum,
    serial_consistency:   nil,
    reusable:             true,
    named:                false,
    page_size:            100,
    page_state:           nil,
    value_encode_handler: nil
  ]

  @callback from_opts(Keyword.t) :: Queryable.t

  defmacro __using__(_opts) do
    quote do
      import Schemata.Query.Sigil
      alias Schemata.CassandraError
      alias Schemata.Result
      alias Schemata.Query
    end
  end

  def new(stmt), do: %Query{statement: stmt}

  def get(%Query{values: values}, key, default \\ nil) do
    values = values || %{}
    Map.get(values, key, default)
  end

  def put(q = %Query{values: values}, key, value) do
    values = values || %{}
    %{q | values: Map.put(values, key, value)}
  end

  def delete(q = %Query{values: values}, key) do
    values = values || %{}
    %{q | values: Map.delete(values, key)}
  end

  def merge(q = %Query{values: values}, other) do
    values = values || %{}
    %{q | values: Map.merge(values, other)}
  end

  def statement(q = %Query{}, statement) do
    %{q | statement: statement}
  end

  def page_size(q = %Query{}, page_size) when is_integer(page_size) do
    %{q | page_size: page_size}
  end

  def consistency(q = %Query{}, consistency) do
    %{q | consistency: consistency}
  end

  def serial_consistency(q = %Query{}, serial_consistency) do
    %{q | serial_consistency: serial_consistency}
  end

  @doc """
  Execute a query.

  `keyspace` is the keyspace to execute the query in.

  `statement` is a query string where '?' characters are substituted with
  parameters from the `Values' list.

  `values` is a property list of column name, value pairs. The pairs must be
  in the same order that the columns are listed in `Query'.

  On successful completion, the function returns `{:ok, :void}` when there are
  no results to return and `{:ok, result}` when there is. `result` is an
  abstract datatype that can be passed to functions in the Result module.
  """
  @spec run(keyspace, statement, values, consistency_level) :: query_result
  def run(keyspace, statement, values, consistency) do
    run(
      %Query{
        statement: statement,
        values: values,
        keyspace: keyspace,
        consistency: consistency
      })
  end

  @doc ""
  @spec run(Query.t | Queryable.t) :: query_result
  def run(%Query{} = query), do: query |> to_cql_query |> CQErl.run_query
  def run(queryable), do: queryable |> to_query |> run

  @doc ""
  @spec run!(Query.t | Queryable.t) :: :void | Result.t
  def run!(query) do
    case run(query) do
      {:ok, result} -> result
      {:error, :no_clients} ->
        raise CassandraError, [
          query: Queryable.statement(query),
          message: "No clients available"
        ]
      {:error, {:error, {reason, stacktrace}}} ->
        raise CassandraError, [
          query: Queryable.statement(query),
          message: "CQErl processing error: #{reason}",
          stack: stacktrace
        ]
      {:error, {code, msg, _extras}} ->
        raise CassandraError, [
          query: Queryable.statement(query),
          message: msg,
          code: code
        ]
    end
  end

  @doc """
  Executes a batch query.

  In a batch query, multiple queries are executed atomically, but not in
  isolation. Note that it is a bad idea to update multiple rows on the same
  table with a batch for performance reasons. This will likely result in worse
  performance. Executing the same query multiple times with different data sets
  performs best when normal prepared queries are used (see
  {@link multi_query/4})

  This feature is meant for updating multiple tables containing the same
  denormalized data atomically.

  `keyspace` is the keyspace to execute the query in.

  `queries` is a list of statement/values pairs. The first element is a query
  string where '?' characters are substituted with parameters from the entries
  in the values list. The values list is a property list of column name,
  value pairs. The pairs must be in the same order that the columns are
  listed in the query.

  On successful completion, the function returns `{:ok, :void}`.
  """
  @spec run_batch(keyspace, [{statement, values}], consistency_level) ::
    {:ok, :void} | {:error, error}
  # Cassandra throws an exception if you try to batch zero queries.
  def run_batch(_keyspace, [], _consistency), do: {:ok, :void}
  def run_batch(keyspace, query_list, consistency) do
    query_list
    |> to_cql_query_batch(keyspace, consistency)
    |> CQErl.run_query
  end

  @doc """
  Executes a query statement multiple times with different datasets.

  `keyspace` is the keyspace to execute the query in.

  `statement` is a query statement where '?' characters are substituted with
  parameters from the entries in the `values` list.

  `values` is a list where each element is a property list of column name,
  value pairs. The pairs must be in the same order that the columns are
  listed in `statement'.

  Returns `:ok`.
  """
  @spec run_multi(keyspace, statement, [values], consistency_level) :: :ok
  def run_multi(keyspace, statement, values_list, consistency) do
    values_list
    |> Enum.each(&run(keyspace, statement, &1, consistency))

    :ok
  end

  @doc """
  Executes multiple queries statements with different datasets.

  This is functionally similar to {@link batch_query/4}, however this
  function executes the queries individually rather than using C*'s batching
  system. This is more appropriate (and performant) when operating over
  multiple bits of data on a single table requiring varied query strings. An
  example may be where certain rows of data require a TTL setting but others do
  not. Don't be afraid to use this form if the query strings may be all
  identical - in this case it will perform exactly the same as {@link
  multi_query/4} (with the relatively small exception of the extra cost of
  passing multiple copies of the same query string).

  `keyspace` is the keyspace to execute the query in.

  `query_vals` is a list of tuples `{statement, values}` where `statement` and
  `values` are as for {@link query/4}.

  Returns `:ok`.
  """
  @spec run_multi(keyspace, [{statement, values}], consistency_level) :: :ok
  def run_multi(keyspace, query_vals, consistency) do
    query_vals
    |> Enum.each(fn {s, v} -> run(keyspace, s, v, consistency) end)

    :ok
  end

  @doc ""
  @spec to_query(Queryable.t) :: Query.t
  def to_query(struct) do
    query = %Query{
      statement:   Queryable.statement(struct),
      values:      Queryable.values(struct),
      keyspace:    Queryable.keyspace(struct)
    }

    c = Queryable.consistency(struct)
    if c, do: %Query{query | consistency: c}, else: query
  end

  defp to_cql_query(query) do
    log_query(query.statement, query.values)
    cql_query(
      statement:            query.statement,
      values:               nullify(query.values, :undefined),
      keyspace:             nullify(query.keyspace, :undefined),
      consistency:          nullify(query.consistency, :undefined),
      serial_consistency:   nullify(query.serial_consistency, :undefined),
      reusable:             nullify(query.reusable, :undefined),
      named:                nullify(query.named, :undefined),
      page_size:            nullify(query.page_size, :undefined),
      page_state:           nullify(query.page_state, :undefined),
      value_encode_handler: nullify(query.value_encode_handler, :undefined)
    )
  end

  defp to_cql_query_batch(query_list, keyspace, consistency) do
    cql_query_batch(
      queries:     batch_query_list(keyspace, query_list),
      consistency: consistency,
      mode:        :logged
    )
  end

  defp batch_query_list(query_list, keyspace) do
    Enum.map(query_list, fn ({statement, values}) ->
      log_query(statement, values)
      cql_query(
        statement: statement,
        values:    nullify(values, :undefined),
        keyspace:  nullify(keyspace, :undefined)
      )
    end)
  end

  defp log_query(statement, %{}) do
    :ok = Logger.debug("Creating CQL query with statement '#{statement}'")
  end
  defp log_query(statement, values) do
    :ok = Logger.debug("""
    Creating CQL query with statement '#{statement}' \
    and values #{inspect(values)}\
    """)
  end

  defp nullify(rec), do: nullify(rec, :null)
  defp nullify(rec, _fallback) when is_map(rec) do
    rec
    |> Enum.map(fn {k, v} -> {k, nullify(v)} end)
    |> Enum.into(%{})
  end
  defp nullify(list = [{_key, _value} | _rest], _fallback) do
    list
    |> Enum.map(fn {k, v} -> {k, nullify(v)} end)
  end
  defp nullify(list = [_value | _rest], fallback) do
    list
    |> Enum.map(&nullify(&1, fallback))
  end
  defp nullify(nil, fallback), do: fallback
  defp nullify(other, _fallback), do: other

  defimpl Queryable do
    def statement(struct),   do: struct.statement
    def values(struct),      do: struct.values
    def keyspace(struct),    do: struct.keyspace
    def consistency(struct), do: struct.consistency
  end

  defmodule Sigil do
    @moduledoc ""
    def sigil_q(statement, _modifiers) do
      %Query{statement: statement}
    end
  end
end
