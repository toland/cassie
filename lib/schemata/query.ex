defmodule Schemata.Query do
  @moduledoc ""

  defmacro __using__(_opts) do
    quote do
      alias Schemata.Query
      alias Schemata.Query.Select
    end
  end

  require Record
  import Record, only: [defrecordp: 2, extract: 2]

  defrecordp :cql_query, extract(:cql_query, from_lib: "cqerl/include/cqerl.hrl")
  defrecordp :cql_result, extract(:cql_result, from_lib: "cqerl/include/cqerl.hrl")

  @type keyspace          :: nil | binary
  @type table             :: atom | binary
  @type explicit_columns  :: [atom]
  @type columns           :: :all | explicit_columns
  @type conditions        :: map

  @type consistency_level :: :any
                           | :one
                           | :two
                           | :three
                           | :quorum
                           | :all
                           | :local_quorum
                           | :each_quorum
                           | :local_one

  @type statement         :: iodata
  @type parameter_val     :: number | binary | list | atom | boolean
  @type value             :: parameter_val
  @type values            :: map
  @type limit             :: nil | pos_integer
  @type row               :: map
  @type rows              :: [row]
  @type error             :: term
  @opaque result          :: record(:cql_result)

  alias Schemata.Query

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
  @spec run(keyspace, statement, values, consistency_level) ::
    {:ok, :void | result} | {:error, error}
  def run(keyspace, statement, values, consistency) do
    run %Query{
      keyspace: keyspace,
      statement: statement,
      values: values,
      consistency: consistency
    }
  end

  @doc ""
  @spec run(Query.t | Queryable.t) :: {:ok, :void | result} | {:error, error}
  def run(%Query{} = query) do
    hosts = Application.get_env(:schemata, :cassandra_hosts)
    opts = Application.get_env(:schemata, :cassandra_opts)
    case get_client(query.keyspace, hd(hosts), opts) do
      {:ok, client} ->
        return = :cqerl.run_query(client, to_cql_query(query))
        :cqerl.close_client(client)
        return
      {:error, error} ->
        {:error, error}
    end
  end
  def run(queryable) do
    queryable
    |> Queryable.to_query
    |> run
  end

  defp get_client(nil, host, opts), do: :cqerl.get_client(host, opts)
  defp get_client(keyspace, host, opts) do
    :cqerl.get_client(host, [{:keyspace, keyspace}|opts])
  end

  defp to_cql_query(query) do
    cql_query(
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
  def all_rows(result), do: :cqerl.all_rows(result)
end
