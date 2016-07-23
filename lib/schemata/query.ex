defmodule Schemata.Query do
  @moduledoc ""

  use Schemata.CQErl
  alias Schemata.Query
  alias Schemata.Queryable
  alias Schemata.Result
  alias Schemata.CassandraError

  @type keyspace          :: nil | binary
  @type table             :: atom | binary
  @type column            :: atom | binary
  @type explicit_columns  :: [column]
  @type columns           :: :all | explicit_columns
  @type datatype          :: CQErl.datatype
  @type conditions        :: map
  @type consistency_level :: CQErl.consistency_level
  @type statement         :: CQErl.query_statement
  @type parameter_val     :: CQErl.parameter_val
  @type value             :: CQErl.parameter_val
  @type values            :: map
  @type limit             :: nil | pos_integer
  @type error             :: {:error, term}
  @type query_result      :: CQErl.query_result

  @type column_def        :: [{atom, datatype
                                   | {:set | :list, datatype}
                                   | {:map, datatype, datatype}}]
  @type primary_key       :: atom | [[atom] | atom]
  @type ordering          :: atom | [{atom, :asc | :desc}]
  @type ks_strategy       :: :simple | :network_topology
  @type ks_factor         :: {atom | binary, non_neg_integer}
                           | non_neg_integer

  @type t :: %Query{
    statement:   binary,
    values:      values,
    keyspace:    keyspace,
    consistency: consistency_level
  }

  @enforce_keys [:statement]
  defstruct [
    statement:   nil,
    values:      %{},
    keyspace:    nil,
    consistency: :quorum
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
    %Query{
      statement:   Queryable.statement(struct),
      values:      Queryable.values(struct),
      keyspace:    Queryable.keyspace(struct),
      consistency: Queryable.consistency(struct)
    }
  end

  defp to_cql_query(struct) do
    query =
      struct
      |> Map.from_struct
      |> Enum.reject(fn {_, v} -> is_nil(v) end)

    cql_query(
      statement:   Keyword.get(query, :statement),
      values:      Keyword.get(query, :values, %{}),
      keyspace:    Keyword.get(query, :keyspace, :undefined),
      consistency: Keyword.get(query, :consistency, :quorum),
      reusable:    true
    )
  end

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
