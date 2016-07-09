defmodule Schemata do
  @moduledoc ""

  use Schemata.Query

  #   %Select{values: :all,
  #           from: "user", in: "wocky_shared",
  #           where: %{server: "foo"},
  #           limit: 1}
  #   |> Query.run
  #   |> Query.first_row

  @doc """
  Retrieves data from a table based on the parameters and returns all rows
  of the result set.

    select :all,
      from: "wocky_shared.user",
      where: %{server: "foo"},
      limit: 1

    select :all,
      from: "user", in: "wocky_shared",
      where: %{server: "foo"},
      limit: 1
      with: :quorum
  """
  @spec select(Query.columns, map) :: Query.rows
  def select(columns, query) do
    select(query[:in],
      Keyword.fetch!(query, :from), columns,
      Keyword.get(query, :where, %{}), query[:limit])
  end

  @doc """
  Retrieves data from a table based on the parameters and returns all rows
  of the result set.
  """
  @spec select(Query.keyspace, Query.table, Query.columns,
               Query.conditions, Query.limit) :: Query.rows
  def select(keyspace, table, columns, conditions, limit \\ nil) do
    %Select{
      values: columns,
      from: table, in: keyspace,
      where: conditions,
      limit: limit
    }
    |> Query.run!
    |> Query.all_rows
  end
end
