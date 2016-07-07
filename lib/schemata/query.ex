defmodule Schemata.Query do
  alias Schemata.Query.Builder

  require Record
  import Record, only: [defrecordp: 2, extract: 2]

  defrecordp :cql_query, extract(:cql_query, from_lib: "cqerl/include/cqerl.hrl")
  defrecordp :cql_result, extract(:cql_result, from_lib: "cqerl/include/cqerl.hrl")

  @type keyspace          :: binary
  @type table             :: atom
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

  @type query             :: iodata
  @type parameter_val     :: number | binary | list | atom | boolean
  @type value             :: parameter_val
  @type values            :: map
  @type row               :: map
  @type rows              :: [row]
  @type error             :: term
  @opaque result          :: record(:cql_result)


  ## @doc Retrieves data from a table based on the parameters and
  ## returns all rows of the result set.
  @spec select(keyspace, table, columns, conditions) :: rows
  def select(keyspace, table, columns, conditions) do
      {:ok, result} = run_select_query(keyspace, table, columns, conditions)
      rows(result)
  end

  defp run_select_query(keyspace, table, columns, conditions, limit \\ :none) do
      query = Builder.select_query(table, columns, keys(conditions), limit)
      query(keyspace, query, conditions, :quorum)
  end

  defp keys(map), do: Map.keys(map)

  ## @doc Execute a query.
  ##
  ## `Context' is the context to execute the query in.
  ##
  ## `Query' is a query string where '?' characters are substituted with
  ## parameters from the `Values' list.
  ##
  ## `Values' is a property list of column name, value pairs. The pairs must be
  ## in the same order that the columns are listed in `Query'.
  ##
  ## On successful completion, the function returns `{ok, void}' when there are
  ## no results to return and `{ok, Result}' when there is. `Result' is an
  ## abstract datatype that can be passed to {@link rows/1} or
  ## {@link single_result/1}.
  ##
  @spec query(keyspace, query, values, consistency_level) ::
    {:ok, :void} | {:ok, result} | {:error, error}
  def query(keyspace, query, values, consistency) do
    run_query(keyspace, make_query(query, values, consistency))
  end

  defp run_query(keyspace, query) do
    nodes = Application.get_env(:schemata, :cassandra_nodes)
    opts = Application.get_env(:schemata, :cassandra_opts)
    case get_client(keyspace, hd(nodes), opts) do
      {:ok, client} ->
        return = :cqerl.run_query(client, query)
        :cqerl.close_client(client)
        return
      {:error, error} ->
        {:error, error}
    end
  end

  defp get_client(:none, node, opts), do: :cqerl.get_client(node, opts)
  defp get_client(keyspace, node, opts), do: :cqerl.get_client(node, [{:keyspace, keyspace}|opts])

  defp make_query(query, values, consistency) do
    cql_query(statement: query,
              values: values,
              reusable: true,
              consistency: consistency)
  end

  ## @doc Extracts rows from a query result
  ##
  ## Returns a list of rows. Each row is a property list of column name, value
  ## pairs.
  ##
  @spec rows(result) :: rows
  def rows(result), do: :cqerl.all_rows(result)

end
