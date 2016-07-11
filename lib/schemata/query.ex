defmodule Schemata.Query do
  @moduledoc ""

  defmacro __using__(_opts) do
    quote do
      alias Schemata.Query
      alias Schemata.Query.Select
    end
  end

  use Schemata.CQErl
  alias Schemata.Query

  @type keyspace          :: nil | binary
  @type table             :: atom | binary
  @type explicit_columns  :: [atom]
  @type columns           :: :all | explicit_columns
  @type conditions        :: map
  @type consistency_level :: CQErl.consistency_level
  @type statement         :: CQErl.query_statement
  @type parameter_val     :: CQErl.parameter_val
  @type value             :: CQErl.parameter_val
  @type values            :: map
  @type limit             :: nil | pos_integer
  @type row               :: map
  @type rows              :: [row]
  @type error             :: {:error, term}
  @opaque result          :: record(:cqerl_result)
  @type query_result      :: CQErl.query_result

  @enforce_keys [:statement]
  defstruct [
    statement:   nil,
    values:      %{},
    keyspace:    nil,
    consistency: :quorum
  ]

  @type t :: %Query{
    statement:   binary,
    values:      values,
    keyspace:    keyspace,
    consistency: consistency_level
  }

  defprotocol Queryable do
    @doc "Converts the struct to a CQL query"
    def to_query(struct)
  end

  @doc """
  Execute a query.

  `Context' is the context to execute the query in.

  `Query' is a query string where '?' characters are substituted with
  parameters from the `Values' list.

  `Values' is a property list of column name, value pairs. The pairs must be
  in the same order that the columns are listed in `Query'.

  On successful completion, the function returns `{ok, void}' when there are
  no results to return and `{ok, Result}' when there is. `Result' is an
  abstract datatype that can be passed to {@link rows/1} or
  {@link single_result/1}.
  """
  @spec run(keyspace, statement, values, consistency_level) :: query_result
  def run(keyspace, statement, values, consistency) do
    run %Query{
      keyspace: keyspace,
      statement: statement,
      values: values,
      consistency: consistency
    }
  end

  @doc ""
  @spec run!(Query.t | Queryable.t) :: :void | result
  def run!(query) do
    {:ok, result} = run(query)
    result
  end

  @doc ""
  @spec run(Query.t | Queryable.t) :: query_result
  def run(%Query{} = query), do: CQErl.run_query(to_cql_query(query))
  def run(queryable), do: queryable |> Queryable.to_query |> run

  defp to_cql_query(query) do
    cql_query(
      keyspace:    query.keyspace
      statement:   query.statement,
      values:      query.values,
      consistency: query.consistency,
      reusable:    true
    )
  end

  @doc """
  Extracts rows from a query result

  Returns a list of rows. Each row is a property list of column name, value
  pairs.
  """
  @spec all_rows(result) :: rows
  def all_rows(result), do: CQErl.all_rows(result)
end
