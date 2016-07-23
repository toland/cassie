defmodule Schemata.Query do
  @moduledoc ""

  use Schemata.CQErl
  alias Schemata.Query
  alias Schemata.Queryable
  alias Schemata.Result
  alias Schemata.CassandraError

  @type keyspace           :: nil | binary
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

  @doc ""
  @spec run(Query.t | Queryable.t) :: query_result
  def run(%Query{} = query), do: query |> to_cql_query |> CQErl.run_query
  def run(queryable), do: queryable |> to_query |> run

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
